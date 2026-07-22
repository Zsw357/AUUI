using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AUUI
{
    /// <summary>
    /// 多模态交互套件socket通信AIUI消息解析demo
    /// 数据协议同串口通信协议，具体可以参考文档：https://aiui-doc.xf-yun.com/project-2/doc-399/
    /// </summary>
    internal class Program
    {
        // iat结果缓存，用于缓存指定sid的所有识别结果帧结果，用于动态修正
        private static Dictionary<string, List<string>> aiuiIatResultCache = new Dictionary<string, List<string>>();

        public static async Task Main(string[] args)
        {
            string ip = "192.168.1.240";
            int port = 19199;

            try
            {
                using (var socket = new TcpClient())
                {
                    await socket.ConnectAsync(ip, port);
                    Console.WriteLine("连接建立成功！");

                    using (var networkStream = socket.GetStream())
                    {
                        while (true)
                        {
                            var message = ReadFrame(networkStream);
                            if (message.MsgLen <= 0)
                            {
                                Thread.Sleep(3000);
                                continue;
                            }

                            if (0xA5 != message.Header || 0x01 != message.UserId)
                            {
                                Console.WriteLine("不支持的标头或用户ID");
                                continue;
                            }

                            byte[] content = message.Content;
                            int msgType = message.MsgType;
                            int msgId = message.MsgId;

                            switch (msgType)
                            {
                                case 0x01:
                                    // 握手请求
                                    Console.WriteLine("连接建立成功！");
                                    break;

                                case 0x04:
                                    // AIUI消息
                                    byte[] originalData = CommonUtil.Decompress(content);
                                    JObject aiuiMessage = JObject.Parse(Encoding.UTF8.GetString(originalData));
                                    string type = aiuiMessage["type"]?.ToString();

                                    if (string.Equals("aiui_event", type))
                                    {
                                        ParseAiuiEvent(aiuiMessage);
                                    }
                                    else if (string.Equals("speech_device_status", type))
                                    {
                                        Console.WriteLine($"多模态设备状态：{aiuiMessage.ToString(Formatting.None)}");
                                    }
                                    else
                                    {
                                        Console.WriteLine($"aiui message type:{type}");
                                    }
                                    break;

                                default:
                                    Console.WriteLine($"unsupport msgType: {msgType}");
                                    continue;
                            }

                            byte[] confirmMessage = MakeConfirmMsg(msgId);
                            await networkStream.WriteAsync(confirmMessage, 0, confirmMessage.Length);
                            await networkStream.FlushAsync();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"异常: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }

        /// <summary>
        /// 解析AIUI结果事件
        /// </summary>
        private static void ParseAiuiEvent(JObject aiuiMessage)
        {
            JObject content = aiuiMessage["content"] as JObject;
            if (content == null) return;

            int? eType = content["eventType"]?.Value<int>();
            if (eType == 2)
            {
                Console.WriteLine($"AIUI错误信息：{aiuiMessage.ToString(Formatting.None)}");
                return;
            }

            if (eType == 4)
            {
                Console.WriteLine($"唤醒事件触发：{content.ToString(Formatting.None)}");
                return;
            }

            if (eType != 1) return;

            // 解析sub
            var so = content.SelectToken("$.info.data[0].params.sub");
            if (so == null) return;

            string sub = so.ToString();
            JObject result = content["result"] as JObject;
            if (result == null) return;

            string sid = result["sid"]?.ToString();
            if (string.IsNullOrEmpty(sid)) return;

            switch (sub)
            {
                case "iat":
                    HandleIatResult(result, sid);
                    break;

                case "cbm_semantic":
                    HandleSemanticResult(result);
                    break;

                case "cbm_knowledge":
                    Console.WriteLine($"文档问答知识库检索结果：{result.ToString(Formatting.None)}");
                    break;

                case "nlp":
                    HandleNlpResult(result);
                    break;

                case "keywords":
                    Console.WriteLine($"语音唤醒结果：{result.ToString(Formatting.None)}");
                    break;

                case "tts":
                    // tts 结果不会抛出，不处理
                    break;

                default:
                    Console.WriteLine($"其他语义结果：{sub}");
                    break;
            }
        }

        private static void HandleIatResult(JObject result, string sid)
        {
            if (!aiuiIatResultCache.ContainsKey(sid))
            {
                aiuiIatResultCache[sid] = new List<string>();
            }

            var iatRes = aiuiIatResultCache[sid];

            // 敏感词检测结果
            string filtered = result["filtered"]?.ToString();
            if (filtered != null)
            {
                Console.WriteLine($"敏感词检测后的识别结果：{filtered}");
            }

            // 当前帧的识别结果
            StringBuilder curIatText = new StringBuilder();
            JObject iatResText = result["text"] as JObject;
            if (iatResText != null)
            {
                JArray wss = iatResText["ws"] as JArray;
                if (wss != null)
                {
                    foreach (var ws in wss)
                    {
                        JArray cws = ws["cw"] as JArray;
                        if (cws != null)
                        {
                            foreach (var cs in cws)
                            {
                                curIatText.Append(cs["w"]?.ToString());
                            }
                        }
                    }
                }

                string pgs = iatResText["pgs"]?.ToString();
                if (string.Equals("rpl", pgs))
                {
                    JArray rg = iatResText["rg"] as JArray;
                    if (rg != null && rg.Count >= 2)
                    {
                        int start = rg[0].Value<int>();
                        int end = rg[1].Value<int>();
                        for (int i = start - 1; i < end && i < iatRes.Count; i++)
                        {
                            iatRes[i] = "";
                        }
                    }
                }
            }

            iatRes.Add(curIatText.ToString());

            bool? ls = iatResText?["ls"]?.Value<bool>();
            if (ls == true)
            {
                Console.WriteLine($"最终识别结果: {string.Join("", iatRes)}");
                aiuiIatResultCache.Remove(sid);
            }
            else
            {
                Console.WriteLine($"识别中间结果: {string.Join("", iatRes)}");
            }
        }

        private static void HandleSemanticResult(JObject result)
        {
            JObject semantic = result["cbm_semantic"] as JObject;
            if (semantic == null) return;

            JObject semanticJson = JObject.Parse(semantic["text"]?.ToString());
            int rc = semanticJson["rc"]?.Value<int>() ?? -1;
            string semanticText = semanticJson["text"]?.ToString();
            string service = semanticJson["service"]?.ToString();
            string category = semanticJson["category"]?.ToString();

            if (rc == 0)
            {
                var answer = semanticJson.SelectToken("$answer.text");
                Console.WriteLine($"技能结果，用户说法：{semanticText}，服务：{service}，命中技能：{category}，技能回复：{answer}");
            }
            else
            {
                Console.WriteLine($"技能结果：{semantic["text"]}");
            }
        }

        private static void HandleNlpResult(JObject result)
        {
            JObject nlp = result["nlp"] as JObject;
            if (nlp == null) return;

            string nlpText = nlp["text"]?.ToString();
            if (string.IsNullOrWhiteSpace(nlpText)) return;

            Console.WriteLine($"语义结果：{nlpText}");
        }

        /// <summary>
        /// 读取一个消息帧
        /// </summary>
        private static Message ReadFrame(NetworkStream stream)
        {
            try
            {
                byte[] meta = ReadExpectLengthData(stream, 7);

                byte header = meta[0];
                byte userId = meta[1];
                byte msgType = meta[2];
                int msgLen = FromLittleEndianHex(meta.Skip(3).Take(2).ToArray());
                int msgId = FromLittleEndianHex(meta.Skip(5).Take(2).ToArray());

                byte[] msgContent = ReadExpectLengthData(stream, msgLen);
                int code = stream.ReadByte();

                if (!CheckCode(meta, msgContent, code))
                {
                    Console.WriteLine("校验码不匹配！");
                    return new Message();
                }

                return new Message(header, userId, msgType, msgLen, msgId, msgContent, code);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"读取消息异常: {ex.Message}");
                return new Message();
            }
        }

        private static byte[] ReadExpectLengthData(NetworkStream stream, int len)
        {
            byte[] data = new byte[len];
            int read = 0;
            while (read < len)
            {
                int r = stream.Read(data, read, len - read);
                if (r == 0)
                {
                    throw new Exception("read 0 socket closed");
                }
                read += r;
            }
            return data;
        }

        private static bool CheckCode(byte[] meta, byte[] msgContent, int code)
        {
            byte[] data = CommonUtil.MergeArray(meta, msgContent);
            int calCode = CalCode(data);
            return calCode == code;
        }

        private static int CalCode(byte[] data)
        {
            int sum = 0;
            foreach (byte b in data)
            {
                sum += b;
            }
            return (~sum + 1) & 0xFF;
        }

        public static byte[] MakeConfirmMsg(int msgId)
        {
            using (var data = new MemoryStream())
            {
                byte[] messageContent = new byte[] { 0xA5, 0, 0, 0 };
                data.WriteByte(0xA5);
                data.WriteByte(0x01);
                data.WriteByte(0xFF);
                data.Write(ToLittleEndianHex(messageContent.Length), 0, 2);
                data.Write(ToLittleEndianHex(msgId), 0, 2);
                data.Write(messageContent, 0, messageContent.Length);
                data.WriteByte((byte)CalCode(data.ToArray()));
                return data.ToArray();
            }
        }

        private static int FromLittleEndianHex(byte[] data)
        {
            return (data[1] << 8) | (data[0] & 0xFF);
        }

        private static byte[] ToLittleEndianHex(int value)
        {
            return new byte[] { (byte)(value & 0xFF), (byte)((value >> 8) & 0xFF) };
        }
    }

    /// <summary>
    /// 消息实体类
    /// </summary>
    public class Message
    {
        public byte Header { get; set; }
        public byte UserId { get; set; }
        public byte MsgType { get; set; }
        public int MsgLen { get; set; }
        public int MsgId { get; set; }
        public byte[] Content { get; set; }
        public int Code { get; set; }

        public Message() { }

        public Message(byte header, byte userId, byte msgType, int msgLen, int msgId, byte[] content, int code)
        {
            Header = header;
            UserId = userId;
            MsgType = msgType;
            MsgLen = msgLen;
            MsgId = msgId;
            Content = content;
            Code = code;
        }
    }

    /// <summary>
    /// 工具类
    /// </summary>
    public static class CommonUtil
    {
        public static byte[] Decompress(byte[] data)
        {
            using (var compressedStream = new MemoryStream(data))
            using (var decompressStream = new DeflateStream(compressedStream, CompressionMode.Decompress))
            using (var resultStream = new MemoryStream())
            {
                decompressStream.CopyTo(resultStream);
                return resultStream.ToArray();
            }
        }

        public static byte[] MergeArray(byte[] first, byte[] second)
        {
            byte[] result = new byte[first.Length + second.Length];
            Buffer.BlockCopy(first, 0, result, 0, first.Length);
            Buffer.BlockCopy(second, 0, result, first.Length, second.Length);
            return result;
        }
    }
}
