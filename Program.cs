using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net.Sockets;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AUUI
{
    /// <summary>
    /// 多模态交互套件socket通信AIUI消息解析demo
    /// 数据协议同串口通信协议，具体可以参考文档：https://aiui-doc.xf-yun.com/project-2/doc-399/
    /// 通信规则：
    /// 1. 双向通信，不分主从机，双方都可向对方发送请求信息
    /// 2. 握手信号：双方发送握手信号，确定与对方串口通信是否正常
    /// 3. 握手重发：未接收到响应时，每隔100ms发送一次握手请求信号
    /// 4. 响应时间：接收方接到请求后需在50ms内发送响应
    /// 5. 超时重发：除确认消息外，发送方发送完成后若超过300ms未得到响应则判定为超时进行重发，重发次数为3次
    /// </summary>
    internal class Program
    {
        // 消息类型定义
        private const byte MSG_TYPE_HANDSHAKE_REQUEST = 0x01;   // 握手请求
        private const byte MSG_TYPE_HANDSHAKE_RESPONSE = 0x02; // 握手响应
        private const byte MSG_TYPE_AIUI_CONFIG = 0x03;        // AIUI配置消息
        private const byte MSG_TYPE_AIUI = 0x04;               // AIUI消息
        private const byte MSG_TYPE_MASTER_CONTROL = 0x05;     // 主控消息
        private const byte MSG_TYPE_NORMAL_REQUEST = 0x05;     // 普通请求
        private const byte MSG_TYPE_NORMAL_RESPONSE = 0x06;    // 普通响应
        private const byte MSG_TYPE_CONFIRM = 0xFF;            // 确认消息

        // 通信参数
        private const int HANDSHAKE_INTERVAL_MS = 100;         // 握手重发间隔100ms
        private const int RESPONSE_TIMEOUT_MS = 300;           // 响应超时300ms
        private const int MAX_RETRY_COUNT = 3;                 // 最大重发次数
        private const int RESPONSE_DEADLINE_MS = 50;           // 响应时限50ms

        // 握手状态管理
        private static bool isHandshakeCompleted = false;
        private static readonly object handshakeLock = new object();
        private static ManualResetEventSlim handshakeEvent = new ManualResetEventSlim(false);

        // 流访问锁（NetworkStream 不是线程安全的，写和读需要互斥）
        private static readonly SemaphoreSlim streamWriteLock = new SemaphoreSlim(1, 1);

        // 待确认消息队列
        private static ConcurrentDictionary<int, PendingMessage> pendingMessages = new ConcurrentDictionary<int, PendingMessage>();

        // 消息ID生成器
        private static int currentMsgId = 0;
        private static readonly object msgIdLock = new object();

        // iat结果缓存
        private static Dictionary<string, List<string>> aiuiIatResultCache = new Dictionary<string, List<string>>();

        private static Random random = new Random();

        public static async Task Main(string[] args)
        {
            // 先测试输出消息格式
            //Console.WriteLine("=== 消息格式测试 ===\n");
            //Console.WriteLine("【MakeHandshakeRequest 输出】");
            //byte[] handshakeReq = MakeHandshakeRequest();
            //Console.WriteLine($"  字节数组: {BitConverter.ToString(handshakeReq)}");
            //Console.WriteLine($"  十六进制: {string.Join(" ", handshakeReq.Select(b => $"0x{b:X2}"))}");
            //Console.WriteLine($"  长度: {handshakeReq.Length} 字节");

            //Console.WriteLine("\n【MakeHandshakeResponse 输出】");
            //byte[] handshakeResp = MakeHandshakeResponse(1);
            //Console.WriteLine($"  字节数组: {BitConverter.ToString(handshakeResp)}");

            //Console.WriteLine("\n【MakeConfirmMsg 输出】");
            //byte[] confirm = MakeConfirmMsg(1);
            //Console.WriteLine($"  字节数组: {BitConverter.ToString(confirm)}");

            //Console.WriteLine("\n【MakeNormalRequest 输出】");
            //byte[] normalReq = MakeNormalRequest(2, "test");
            //Console.WriteLine($"  字节数组: {BitConverter.ToString(normalReq)}");

            //Console.WriteLine("\n【MakeNormalResponse 输出】");
            //byte[] normalResp = MakeNormalResponse(2, "ok");
            //Console.WriteLine($"  字节数组: {BitConverter.ToString(normalResp)}");

            //Console.WriteLine("\n=== 连接测试开始 ===");
            string ip = "192.168.31.90";
            int port = 19199;

            try
            {
                using (var socket = new TcpClient())
                {
                    socket.SendTimeout = 1000;
                    socket.ReceiveTimeout = Timeout.Infinite;

                    await socket.ConnectAsync(ip, port);
                    Console.WriteLine("TCP连接建立成功！");

                    using (var networkStream = socket.GetStream())
                    {
                        // 启动消息处理任务（被动接收 AIUI 模块的消息）
                        var receiveTask = RunMessageReceiveLoopAsync(networkStream);

                        // 启动超时检查任务
                        var timeoutTask = RunTimeoutCheckerAsync();

                        // 等待握手完成（AIUI 模块会主动发握手请求 0x01）
                        handshakeEvent.Wait();
                        Console.WriteLine("串口通信握手成功！");

                        // 启动普通请求发送测试（可选）
                        //_ = RunRequestResponseTestAsync(networkStream);

                        // 等待所有任务
                        await Task.WhenAll(receiveTask, timeoutTask);
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
        /// 【已删除】握手循环 —— 上位机不应主动发送握手，应等 AIUI 模块的握手请求。
        /// 原 RunHandshakeLoopAsync 已移除，理由是 Java 参考代码是被动等待握手请求。
        /// </summary>

        /// <summary>
        /// 消息接收循环
        /// </summary>
        private static async Task RunMessageReceiveLoopAsync(NetworkStream stream)
        {
            while (true)
            {
                try
                {
                    var message = ReadFrame(stream);
                    if (message.MsgLen < 0)
                    {
                        await Task.Delay(100);
                        continue;
                    }

                    // 异步处理消息
                    _ = Task.Run(() => HandleMessageAsync(message, stream));
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"接收消息异常: {ex.Message}");
                    await Task.Delay(100);
                }
            }
        }

        /// <summary>
        /// 处理接收到的消息
        /// </summary>
        private static async Task HandleMessageAsync(Message message, NetworkStream stream)
        {
            try
            {
                if (0xA5 != message.Header || 0x01 != message.UserId)
                {
                    Console.WriteLine("不支持的标头或用户ID");
                    return;
                }

                int msgType = message.MsgType;
                int msgId = message.MsgId;
                byte[] content = message.Content ?? Array.Empty<byte>();
                bool needConfirm = false;

                switch (msgType)
                {
                    case MSG_TYPE_HANDSHAKE_REQUEST:  // 0x01 握手请求
                        Console.WriteLine("连接建立成功！");
                        lock (handshakeLock)
                        {
                            if (!isHandshakeCompleted)
                            {
                                isHandshakeCompleted = true;
                                handshakeEvent.Set();
                            }
                        }
                        needConfirm = true;
                        break;

                    case MSG_TYPE_AIUI:  // 0x04 AIUI消息
                        //Console.WriteLine($"  [调试] 收到AIUI消息，长度: {content.Length} 字节");

                        // 协议规定 AIUI消息 内容是 gzip 压缩
                        byte[] originalData = CommonUtil.GZipDecompress(content);
                        //Console.WriteLine($"  [调试] 解压后长度: {originalData.Length} 字节");
                        Console.WriteLine($"  [调试] 解压后内容: {Encoding.UTF8.GetString(originalData)}");

                        JObject aiuiMessage = JObject.Parse(Encoding.UTF8.GetString(originalData));
                        string? type = aiuiMessage["type"]?.ToString();

                        if (string.Equals("aiui_event", type))
                        {
                            await ParseAiuiEventAsync(aiuiMessage, stream);
                        }
                        else if (string.Equals("speech_device_status", type))
                        {
                            Console.WriteLine($"多模态设备状态：{aiuiMessage.ToString(Formatting.None)}");
                        }
                        else
                        {
                            Console.WriteLine($"aiui message type:{type}");
                        }
                        needConfirm = true;
                        break;

                    case MSG_TYPE_NORMAL_REQUEST:  // 0x05 普通请求
                        Console.WriteLine($"收到普通请求，MsgId: {msgId}");
                        needConfirm = true;
                        break;

                    case MSG_TYPE_CONFIRM:  // 0xFF 确认消息
                        Console.WriteLine($"  [调试] 收到确认消息，MsgId: {msgId}");
                        HandleConfirmMessage(message);
                        // 确认消息不需要回复确认
                        break;

                    case MSG_TYPE_NORMAL_RESPONSE:  // 0x06 普通响应
                        Console.WriteLine($"  [调试] 收到普通响应，MsgId: {msgId}");
                        HandleNormalResponse(message);
                        break;

                    default:
                        Console.WriteLine($"unsupport msgType: {msgType}");
                        break;
                }

                // AIUI 客户端惯例：每收到一条请求消息，都要回一条确认（0xFF）
                if (needConfirm)
                {
                    byte[] confirmMessage = MakeConfirmMsg(msgId);
                    await streamWriteLock.WaitAsync();
                    try
                    {
                        await stream.WriteAsync(confirmMessage, 0, confirmMessage.Length);
                        await stream.FlushAsync();
                    }
                    finally
                    {
                        streamWriteLock.Release();
                    }
                    //Console.WriteLine($"  [调试] 已发送确认消息 MsgId={msgId}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"处理消息异常: {ex.Message}");
            }
        }

        /// <summary>
        /// 处理确认消息
        /// </summary>
        private static void HandleConfirmMessage(Message message)
        {
            if (pendingMessages.TryRemove(message.MsgId, out var pending))
            {
                pending.IsConfirmed = true;
                pending.ConfirmEvent.Set();
                Console.WriteLine($"收到MsgId={message.MsgId}的确认消息");
            }
        }

        /// <summary>
        /// 处理普通响应消息
        /// </summary>
        private static void HandleNormalResponse(Message message)
        {
            string responseContent = Encoding.UTF8.GetString(message.Content);
            Console.WriteLine($"收到响应，MsgId={message.MsgId}: {responseContent}");

            if (pendingMessages.TryRemove(message.MsgId, out var pending))
            {
                pending.IsConfirmed = true;
                pending.ResponseContent = responseContent;
                pending.ConfirmEvent.Set();
            }
        }

        /// <summary>
        /// 超时检查循环
        /// </summary>
        private static async Task RunTimeoutCheckerAsync()
        {
            while (true)
            {
                await Task.Delay(50);

                foreach (var kvp in pendingMessages.ToArray())
                {
                    var pending = kvp.Value;
                    if (pending.IsConfirmed) continue;

                    var elapsed = (DateTime.Now - pending.SendTime).TotalMilliseconds;
                    if (elapsed >= RESPONSE_TIMEOUT_MS)
                    {
                        if (pending.RetryCount < MAX_RETRY_COUNT)
                        {
                            Console.WriteLine($"MsgId={pending.MsgId} 超时({elapsed:F0}ms)，进行第{pending.RetryCount + 1}次重发...");
                            pending.RetryCount++;
                            pending.SendTime = DateTime.Now;
                            // 注意：这里需要重新发送，但当前上下文没有stream引用
                            // 实际实现中需要通过其他方式触发重发
                        }
                        else
                        {
                            Console.WriteLine($"MsgId={pending.MsgId} 重发次数已达上限，放弃发送");
                            pending.IsConfirmed = true; // 标记为已处理，避免重复检查
                            pending.ConfirmEvent.Set();
                        }
                    }
                }
            }
        }

       

        /// <summary>
        /// 获取下一个消息ID
        /// </summary>
        private static int GetNextMsgId()
        {
            lock (msgIdLock)
            {
                currentMsgId = (currentMsgId + 1) % 65536;
                return currentMsgId;
            }
        }

        /// <summary>
        /// 解析AIUI结果事件
        /// </summary>
        private static async Task ParseAiuiEventAsync(JObject aiuiMessage, NetworkStream stream)
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
                    await HandleIatResultAsync(result, sid, stream);
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
                    break;

                default:
                    //Console.WriteLine($"其他语义结果：{sub}");
                    break;
            }
        }

        private static async Task HandleIatResultAsync(JObject result, string sid, NetworkStream stream)
        {
            if (!aiuiIatResultCache.ContainsKey(sid))
            {
                aiuiIatResultCache[sid] = new List<string>();
            }

            var iatRes = aiuiIatResultCache[sid];

            string filtered = result["filtered"]?.ToString();
            if (filtered != null)
            {
                Console.WriteLine($"敏感词检测后的识别结果：{filtered}");
            }

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
                string finalText = string.Join("", iatRes);
                Console.WriteLine();
                Console.WriteLine($"最终识别结果: {finalText}");
                Console.WriteLine();

                // 收到最终识别结果 → 自动回复 "好的" 让 AIUI 模块播报
                await SendTtsAsync(stream, "好的", sid: GetNextTtsSid());

                aiuiIatResultCache.Remove(sid);
            }
            else
            {
                Console.WriteLine($"识别中间结果: {string.Join("", iatRes)}");
            }
        }

        private static int ttsSidCounter = 400;

        private static int GetNextTtsSid()
        {
            return Interlocked.Increment(ref ttsSidCounter);
        }

        private static async Task SendTtsAsync(NetworkStream stream, string text, int sid)
        {
            try
            {
                byte[] ttsPacket = MakeTtsRequest(text, sid);
                await streamWriteLock.WaitAsync();
                try
                {
                    await stream.WriteAsync(ttsPacket, 0, ttsPacket.Length);
                    await stream.FlushAsync();
                    Console.WriteLine($"  [TTS] 已发送合成请求: \"{text}\" (sid={sid})");
                }
                finally
                {
                    streamWriteLock.Release();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  [TTS] 发送失败: {ex.Message}");
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

            //Console.WriteLine($"语义结果：{nlpText}");
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

        /// <summary>
        /// 发送握手请求（已废弃）
        /// 【已废弃】Java 参考代码是被动接收 AIUI 模块的握手请求 —— 不由上位机主动发起。
        /// 协议格式: 0xA5 0x01 0x01 [长度] [ID] 0xA5 0x00 0x00 0x00 [校验]
        /// 消息数据固定为 0xA5 0x00 0x00 0x00 (4字节)
        /// </summary>
        [Obsolete("上位机不要主动发握手请求，应等待 AIUI 模块发起的 0x01 握手请求后回复 0xFF 确认。")]
        public static byte[] MakeHandshakeRequest()
        {
            using (var data = new MemoryStream())
            {
                // 握手请求的消息数据固定为 0xA5 0x00 0x00 0x00
                byte[] messageContent = new byte[] { 0xA5, 0x00, 0x00, 0x00 };
                data.WriteByte(0xA5);                                     // 同步头
                data.WriteByte(0x01);                                     // 用户ID
                data.WriteByte(MSG_TYPE_HANDSHAKE_REQUEST);               // 消息类型
                data.Write(ToLittleEndianHex(messageContent.Length), 0, 2); // 消息长度(小端)
                data.Write(ToLittleEndianHex(GetNextMsgId()), 0, 2);      // 消息ID(小端)
                data.Write(messageContent, 0, messageContent.Length);     // 消息数据
                data.WriteByte((byte)CalCode(data.ToArray()));            // 校验码
                return data.ToArray();
            }
        }

        /// <summary>
        /// 发送握手响应（已废弃）
        /// 【已废弃】Java 参考代码从不主动发送握手响应 —— 上位机只需回复确认消息（0xFF）。
        /// 协议格式: 0xA5 0x01 0x02 [长度] [ID] 0xA5 0x00 0x00 0x00 [校验]
        /// 握手响应消息数据固定为 0xA5 0x00 0x00 0x00 (4字节)
        /// </summary>
        [Obsolete("上位机不应主动发送握手响应，仅返回 0xFF 确认消息即可。")]
        public static byte[] MakeHandshakeResponse(int requestMsgId)
        {
            using (var data = new MemoryStream())
            {
                // 握手响应的消息数据固定为 0xA5 0x00 0x00 0x00
                byte[] messageContent = new byte[] { 0xA5, 0x00, 0x00, 0x00 };
                data.WriteByte(0xA5);
                data.WriteByte(0x01);
                data.WriteByte(MSG_TYPE_HANDSHAKE_RESPONSE);
                data.Write(ToLittleEndianHex(messageContent.Length), 0, 2);
                data.Write(ToLittleEndianHex(requestMsgId), 0, 2);
                data.Write(messageContent, 0, messageContent.Length);
                data.WriteByte((byte)CalCode(data.ToArray()));
                return data.ToArray();
            }
        }

        /// <summary>
        /// 发送普通请求
        /// </summary>
        public static byte[] MakeNormalRequest(int msgId, string content)
        {
            using (var data = new MemoryStream())
            {
                byte[] messageContent = Encoding.UTF8.GetBytes(content);
                data.WriteByte(0xA5);
                data.WriteByte(0x01);
                data.WriteByte(MSG_TYPE_NORMAL_REQUEST);
                data.Write(ToLittleEndianHex(messageContent.Length), 0, 2);
                data.Write(ToLittleEndianHex(msgId), 0, 2);
                data.Write(messageContent, 0, messageContent.Length);
                data.WriteByte((byte)CalCode(data.ToArray()));
                return data.ToArray();
            }
        }

        /// <summary>
        /// 发送 TTS 合成请求（主控消息 0x05，内容 GZip 压缩）
        /// 协议格式参考 tts.py：JSON body + GZip 压缩 + AIUI 帧头
        /// </summary>
        public static byte[] MakeTtsRequest(string text, int sid = 400)
        {
            // 构造 JSON 内容（与 tts.py 一致）
            var jsonObject = new JObject
            {
                ["type"] = "tts",
                ["content"] = new JObject
                {
                    ["action"] = "start",
                    ["text"] = text,
                    ["parameters"] = new JObject
                    {
                        ["data_type"] = "text",
                        ["scene"] = "IFLYTEK.tts"
                    },
                    ["status"] = 0,
                    ["index"] = 0
                }
            };

            byte[] jsonBytes = Encoding.UTF8.GetBytes(jsonObject.ToString(Newtonsoft.Json.Formatting.None));
            // AIUI 模块要求主控消息内容使用 GZip 压缩
            byte[] messageContent = CommonUtil.GZipCompress(jsonBytes);

            using (var data = new MemoryStream())
            {
                data.WriteByte(0xA5);                                  // 同步头
                data.WriteByte(0x01);                                  // 用户ID
                data.WriteByte(MSG_TYPE_NORMAL_REQUEST);               // 消息类型 0x05
                data.Write(ToLittleEndianHex(messageContent.Length), 0, 2); // 消息长度(小端)
                data.Write(ToLittleEndianHex(sid), 0, 2);              // 会话ID(小端)
                data.Write(messageContent, 0, messageContent.Length);  // 消息数据(GZip压缩JSON)
                data.WriteByte((byte)CalCode(data.ToArray()));         // 校验码
                return data.ToArray();
            }
        }

        /// <summary>
        /// 发送普通响应
        /// </summary>
        public static byte[] MakeNormalResponse(int requestMsgId, string content)
        {
            using (var data = new MemoryStream())
            {
                byte[] messageContent = Encoding.UTF8.GetBytes(content);
                data.WriteByte(0xA5);
                data.WriteByte(0x01);
                data.WriteByte(MSG_TYPE_NORMAL_RESPONSE);
                data.Write(ToLittleEndianHex(messageContent.Length), 0, 2);
                data.Write(ToLittleEndianHex(requestMsgId), 0, 2);
                data.Write(messageContent, 0, messageContent.Length);
                data.WriteByte((byte)CalCode(data.ToArray()));
                return data.ToArray();
            }
        }

        public static byte[] MakeConfirmMsg(int msgId)
        {
            using (var data = new MemoryStream())
            {
                byte[] messageContent = new byte[] { 0xA5, 0, 0, 0 };
                data.WriteByte(0xA5);
                data.WriteByte(0x01);
                data.WriteByte(MSG_TYPE_CONFIRM);
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
    /// 待确认消息实体
    /// </summary>
    public class PendingMessage
    {
        public int MsgId { get; set; }
        public DateTime SendTime { get; set; }
        public int RetryCount { get; set; }
        public bool IsConfirmed { get; set; }
        public string? ResponseContent { get; set; }
        public ManualResetEventSlim ConfirmEvent { get; set; } = new ManualResetEventSlim(false);
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
        public byte[]? Content { get; set; }
        public int Code { get; set; }

        public Message() { }

        public Message(byte header, byte userId, byte msgType, int msgLen, int msgId, byte[]? content, int code)
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
                if (data == null || data.Length == 0)
                    return data ?? Array.Empty<byte>();

                // 尝试 Deflate 格式解压
                try
                {
                    using (var compressedStream = new MemoryStream(data))
                    using (var decompressStream = new DeflateStream(compressedStream, CompressionMode.Decompress))
                    using (var resultStream = new MemoryStream())
                    {
                        decompressStream.CopyTo(resultStream);
                        return resultStream.ToArray();
                    }
                }
                catch
                {
                    // 尝试 GZip 格式解压
                    try
                    {
                        using (var compressedStream = new MemoryStream(data))
                        using (var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
                        using (var resultStream = new MemoryStream())
                        {
                            gzipStream.CopyTo(resultStream);
                            return resultStream.ToArray();
                        }
                    }
                    catch
                    {
                        // 如果都失败，返回原始数据（可能没有压缩）
                        Console.WriteLine("  [调试] 解压失败，尝试返回原始数据");
                        return data;
                    }
                }
            }

            public static bool IsCompressed(byte[] data)
            {
                if (data == null || data.Length < 2)
                    return false;
                // GZip 魔数: 0x1F 0x8B
                // Zlib 头: 0x78 (最常见)
                return (data[0] == 0x1F && data[1] == 0x8B) ||
                       (data[0] == 0x78);
            }

            public static byte[] Compress(byte[] data)
            {
                using (var resultStream = new MemoryStream())
                using (var compressStream = new DeflateStream(resultStream, CompressionLevel.Optimal))
                {
                    compressStream.Write(data, 0, data.Length);
                    compressStream.Close();
                    return resultStream.ToArray();
                }
            }

            /// <summary>
        /// GZip 解压（AIUI消息使用）
        /// </summary>
            public static byte[] GZipDecompress(byte[] data)
            {
                if (data == null || data.Length == 0)
                    return data ?? Array.Empty<byte>();

                using (var compressedStream = new MemoryStream(data))
                using (var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
                using (var resultStream = new MemoryStream())
                {
                    gzipStream.CopyTo(resultStream);
                    return resultStream.ToArray();
                }
            }

            /// <summary>
            /// GZip 压缩（向 AIUI 模块发送主控消息时使用）
            /// </summary>
            public static byte[] GZipCompress(byte[] data)
            {
                if (data == null || data.Length == 0)
                    return data ?? Array.Empty<byte>();

                using (var resultStream = new MemoryStream())
                using (var gzipStream = new GZipStream(resultStream, CompressionLevel.Optimal))
                {
                    gzipStream.Write(data, 0, data.Length);
                    gzipStream.Close();
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
