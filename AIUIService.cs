using System.Collections.Concurrent;
using System.IO.Compression;
using System.Net.Sockets;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AUUI;

/// <summary>
/// AIUI 多模态交互套件 Socket 通信服务
/// - 自动连接、自动握手
/// - 接收并解析 AIUI 消息（IAT 识别结果、语义结果等）
/// - 提供 TTS 发送接口
/// - 事件回调，Program.cs 通过订阅事件获取结果
/// </summary>
public class AIUIService : IDisposable
{
    #region 协议常量

    private const byte MSG_TYPE_HANDSHAKE_REQUEST = 0x01;
    private const byte MSG_TYPE_HANDSHAKE_RESPONSE = 0x02;
    private const byte MSG_TYPE_AIUI_CONFIG = 0x03;
    private const byte MSG_TYPE_AIUI = 0x04;
    private const byte MSG_TYPE_MASTER_CONTROL = 0x05;
    private const byte MSG_TYPE_NORMAL_REQUEST = 0x05;
    private const byte MSG_TYPE_NORMAL_RESPONSE = 0x06;
    private const byte MSG_TYPE_CONFIRM = 0xFF;

    private const int RESPONSE_TIMEOUT_MS = 300;
    private const int MAX_RETRY_COUNT = 3;

    #endregion

    #region 公共事件

    /// <summary>握手完成后触发</summary>
    public event Action? OnHandshakeCompleted;

    /// <summary>唤醒事件触发时触发，参数为事件内容 JSON</summary>
    public event Action<string>? OnWakeUp;

    /// <summary>IAT 最终识别结果（params.sub == "iat" 且 ls==true），参数为完整文本</summary>
    public event Action<string>? OnIatFinalResult;

    /// <summary>IAT 中间识别结果，参数为当前累计文本</summary>
    public event Action<string>? OnIatPartialResult;

    /// <summary>语义结果（params.sub == "cbm_semantic"），参数为语义 JSON</summary>
    public event Action<JObject>? OnSemanticResult;

    /// <summary>文档问答知识库检索结果，参数为结果 JSON</summary>
    public event Action<JObject>? OnKnowledgeResult;

    /// <summary>NLP 语义结果，参数为 NLP JSON</summary>
    public event Action<JObject>? OnNlpResult;

    /// <summary>多模态设备状态变化，参数为状态 JSON</summary>
    public event Action<string>? OnDeviceStatus;

    /// <summary>AIUI 错误事件，参数为错误信息 JSON</summary>
    public event Action<string>? OnAiuiError;

#pragma warning disable CS0067 // OnTtsCompleted 是公开 API，供外部订阅
    /// <summary>TTS 播放完成（参数为 sid）</summary>
    public event Action<int>? OnTtsCompleted;
#pragma warning restore CS0067


    /// <summary>连接异常时触发，参数为异常信息</summary>
    public event Action<Exception>? OnError;

    #endregion

    #region 私有字段

    private readonly string _ip;
    private readonly int _port;
    private TcpClient? _socket;
    private NetworkStream? _stream;
    private CancellationTokenSource? _cts;

    private bool _isHandshakeCompleted;
    private readonly object _handshakeLock = new();

    private readonly SemaphoreSlim _streamWriteLock = new(1, 1);
    private readonly ConcurrentDictionary<int, PendingMessage> _pendingMessages = new();

    private int _currentMsgId;
    private readonly object _msgIdLock = new();

    private int _ttsSidCounter = 400;

    private readonly Dictionary<string, List<string>> _iatResultCache = new();

    private bool _disposed;

    #endregion

    #region 构造函数 / 生命周期

    public AIUIService(string ip, int port)
    {
        _ip = ip;
        _port = port;
    }

    /// <summary>
    /// 启动 AIUI 服务（连接、握手、接收循环）
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(AIUIService));

        _socket = new TcpClient { SendTimeout = 1000, ReceiveTimeout = Timeout.Infinite };
        await _socket.ConnectAsync(_ip, _port, cancellationToken);
        Console.WriteLine("[AIUIService] TCP 连接建立成功！");

        _stream = _socket.GetStream();
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // 启动接收循环
        _ = RunMessageReceiveLoopAsync(_cts.Token);

        // 等待握手完成
        await WaitForHandshakeAsync(_cts.Token);
        Console.WriteLine("[AIUIService] 串口通信握手成功！");
    }

    /// <summary>
    /// 停止服务并释放资源
    /// </summary>
    public void Stop()
    {
        Dispose();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts?.Cancel();
        try { _stream?.Close(); } catch { }
        try { _socket?.Close(); } catch { }
        _cts?.Dispose();
        _streamWriteLock.Dispose();

        foreach (var p in _pendingMessages.Values)
            p.ConfirmEvent.Dispose();
        _pendingMessages.Clear();
    }

    private async Task WaitForHandshakeAsync(CancellationToken ct)
    {
        var tcs = new TaskCompletionSource();
        using var reg = ct.Register(() => tcs.TrySetCanceled());
        void Handler() => tcs.TrySetResult();
        OnHandshakeCompleted += Handler;
        try
        {
            await tcs.Task;
        }
        finally
        {
            OnHandshakeCompleted -= Handler;
        }
    }

    #endregion

    #region 消息接收与处理

    private async Task RunMessageReceiveLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var message = ReadFrame(_stream!);
                if (message.MsgLen < 0)
                {
                    await Task.Delay(100, ct);
                    continue;
                }
                _ = Task.Run(() => HandleMessageAsync(message, _stream!), ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AIUIService] 接收消息异常: {ex.Message}");
                OnError?.Invoke(ex);
                await Task.Delay(100, ct);
            }
        }
    }

    private async Task HandleMessageAsync(Message message, NetworkStream stream)
    {
        try
        {
            if (0xA5 != message.Header || 0x01 != message.UserId)
            {
                Console.WriteLine("[AIUIService] 不支持的标头或用户ID");
                return;
            }

            int msgType = message.MsgType;
            int msgId = message.MsgId;
            byte[] content = message.Content ?? Array.Empty<byte>();
            bool needConfirm = false;

            switch (msgType)
            {
                case MSG_TYPE_HANDSHAKE_REQUEST:
                    Console.WriteLine("[AIUIService] 连接建立成功！");
                    lock (_handshakeLock)
                    {
                        if (!_isHandshakeCompleted)
                        {
                            _isHandshakeCompleted = true;
                            OnHandshakeCompleted?.Invoke();
                        }
                    }
                    needConfirm = true;
                    break;

                case MSG_TYPE_AIUI:
                    byte[] originalData = CommonUtil.GZipDecompress(content);
                    //Console.WriteLine($"  [调试] 解压后内容: {Encoding.UTF8.GetString(originalData)}");

                    JObject aiuiMessage = JObject.Parse(Encoding.UTF8.GetString(originalData));
                    string? type = aiuiMessage["type"]?.ToString();

                    if (string.Equals("aiui_event", type))
                        await ParseAiuiEventAsync(aiuiMessage);
                    else if (string.Equals("speech_device_status", type))
                        OnDeviceStatus?.Invoke(aiuiMessage.ToString(Formatting.None));
                    else
                        Console.WriteLine($"aiui message type: {type}");

                    needConfirm = true;
                    break;

                case MSG_TYPE_NORMAL_REQUEST:
                    Console.WriteLine($"[AIUIService] 收到普通请求，MsgId: {msgId}");
                    needConfirm = true;
                    break;

                case MSG_TYPE_CONFIRM:
                    //Console.WriteLine($"  [调试] 收到确认消息，MsgId: {msgId}");
                    HandleConfirmMessage(message);
                    break;

                case MSG_TYPE_NORMAL_RESPONSE:
                    Console.WriteLine($"  [调试] 收到普通响应，MsgId: {msgId}");
                    HandleNormalResponse(message);
                    break;

                default:
                    Console.WriteLine($"[AIUIService] unsupport msgType: {msgType}");
                    break;
            }

            if (needConfirm)
            {
                byte[] confirmMessage = MakeConfirmMsg(msgId);
                await _streamWriteLock.WaitAsync();
                try
                {
                    await stream.WriteAsync(confirmMessage, 0, confirmMessage.Length);
                    await stream.FlushAsync();
                }
                finally
                {
                    _streamWriteLock.Release();
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AIUIService] 处理消息异常: {ex.Message}");
            OnError?.Invoke(ex);
        }
    }

    private void HandleConfirmMessage(Message message)
    {
        if (_pendingMessages.TryRemove(message.MsgId, out var pending))
        {
            pending.IsConfirmed = true;
            pending.ConfirmEvent.Set();
        }
    }

    private void HandleNormalResponse(Message message)
    {
        string responseContent = Encoding.UTF8.GetString(message.Content ?? []);
        Console.WriteLine($"[AIUIService] 收到响应，MsgId={message.MsgId}: {responseContent}");

        if (_pendingMessages.TryRemove(message.MsgId, out var pending))
        {
            pending.IsConfirmed = true;
            pending.ResponseContent = responseContent;
            pending.ConfirmEvent.Set();
        }
    }

    #endregion

    #region AIUI 事件解析

    private async Task ParseAiuiEventAsync(JObject aiuiMessage)
    {
        JObject? content = aiuiMessage["content"] as JObject;
        if (content == null) return;

        int? eType = content["eventType"]?.Value<int>();

        if (eType == 2)
        {
            Console.WriteLine($"[AIUIService] AIUI错误信息：{aiuiMessage.ToString(Formatting.None)}");
            OnAiuiError?.Invoke(aiuiMessage.ToString(Formatting.None));
            return;
        }

        if (eType == 4)
        {
            Console.WriteLine($"[AIUIService] 唤醒事件触发：{content.ToString(Formatting.None)}");
            OnWakeUp?.Invoke(content.ToString(Formatting.None));
            return;
        }

        if (eType != 1) return;

        string? sub = content.SelectToken("$.info.data[0].params.sub")?.ToString();
        if (sub == null) return;

        JObject? result = content["result"] as JObject;
        if (result == null) return;

        string? sid = result["sid"]?.ToString();
        if (string.IsNullOrEmpty(sid)) return;

        switch (sub)
        {
            case "iat":
                await HandleIatResultAsync(result, sid);
                break;
            case "cbm_semantic":
                HandleSemanticResult(result);
                break;
            case "cbm_knowledge":
                Console.WriteLine($"[AIUIService] 文档问答知识库检索结果：{result.ToString(Formatting.None)}");
                OnKnowledgeResult?.Invoke(result);
                break;
            case "nlp":
                HandleNlpResult(result);
                break;
            case "keywords":
                Console.WriteLine($"[AIUIService] 语音唤醒结果：{result.ToString(Formatting.None)}");
                break;
            case "tts":
                break;
        }
    }

    private async Task HandleIatResultAsync(JObject result, string sid)
    {
        if (!_iatResultCache.ContainsKey(sid))
            _iatResultCache[sid] = new List<string>();

        var iatRes = _iatResultCache[sid];

        string? filtered = result["filtered"]?.ToString();
        if (filtered != null)
            Console.WriteLine($"[AIUIService] 敏感词检测后的识别结果：{filtered}");

        StringBuilder curIatText = new();
        JObject? iatResText = result["text"] as JObject;
        if (iatResText != null)
        {
            JArray? wss = iatResText["ws"] as JArray;
            if (wss != null)
            {
                foreach (var ws in wss)
                {
                    JArray? cws = ws["cw"] as JArray;
                    if (cws != null)
                    {
                        foreach (var cs in cws)
                            curIatText.Append(cs["w"]?.ToString());
                    }
                }
            }

            string? pgs = iatResText["pgs"]?.ToString();
            if (string.Equals("rpl", pgs))
            {
                JArray? rg = iatResText["rg"] as JArray;
                if (rg != null && rg.Count >= 2)
                {
                    int start = rg[0].Value<int>();
                    int end = rg[1].Value<int>();
                    for (int i = start - 1; i < end && i < iatRes.Count; i++)
                        iatRes[i] = "";
                }
            }
        }

        iatRes.Add(curIatText.ToString());

        bool? ls = iatResText?["ls"]?.Value<bool>();
        if (ls == true)
        {
            string finalText = string.Join("", iatRes);
            Console.WriteLine();
            Console.WriteLine($"[AIUIService] 最终识别结果: {finalText}");
            Console.WriteLine();

            OnIatFinalResult?.Invoke(finalText);
            _iatResultCache.Remove(sid);
        }
        else
        {
            string partial = string.Join("", iatRes);
            Console.WriteLine($"[AIUIService] 识别中间结果: {partial}");
            OnIatPartialResult?.Invoke(partial);
        }
    }

    private void HandleSemanticResult(JObject result)
    {
        JObject? semantic = result["cbm_semantic"] as JObject;
        if (semantic == null) return;

        string? semanticTextRaw = semantic["text"]?.ToString();
        JObject? semanticJson = string.IsNullOrEmpty(semanticTextRaw) ? null : JObject.Parse(semanticTextRaw);
        int rc = semanticJson?["rc"]?.Value<int>() ?? -1;
        string? semanticText = semanticJson?["text"]?.ToString();
        string? service = semanticJson?["service"]?.ToString();
        string? category = semanticJson?["category"]?.ToString();

        if (rc == 0)
        {
            var answer = semanticJson?.SelectToken("$.answer.text");
            Console.WriteLine($"[AIUIService] 技能结果，用户说法：{semanticText}，服务：{service}，命中技能：{category}，技能回复：{answer}");
        }
        else
        {
            Console.WriteLine($"[AIUIService] 技能结果：{semantic["text"]}");
        }

        if (semanticJson != null)
            OnSemanticResult?.Invoke(semanticJson);
    }

    private void HandleNlpResult(JObject result)
    {
        JObject? nlp = result["nlp"] as JObject;
        if (nlp == null) return;

        string? nlpText = nlp["text"]?.ToString();
        if (string.IsNullOrWhiteSpace(nlpText)) return;

        OnNlpResult?.Invoke(nlp);
    }

    #endregion

    #region 对外接口

    /// <summary>
    /// 发送 TTS 合成请求
    /// </summary>
    /// <param name="text">要播放的文本</param>
    /// <param name="onCompleted">播放完成回调（传 sid），可传 null</param>
    public async Task SendTtsAsync(string text, Action<int>? onCompleted = null)
    {
        if (_disposed || _stream == null) return;

        int sid = GetNextTtsSid();
        if (onCompleted != null)
            OnTtsCompleted += h => onCompleted(h);

        try
        {
            byte[] ttsPacket = MakeTtsRequest(text, sid);
            await _streamWriteLock.WaitAsync();
            try
            {
                await _stream.WriteAsync(ttsPacket, 0, ttsPacket.Length);
                await _stream.FlushAsync();
                Console.WriteLine($"[AIUIService] [TTS] 已发送合成请求: \"{text}\" (sid={sid})");
            }
            finally
            {
                _streamWriteLock.Release();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AIUIService] [TTS] 发送失败: {ex.Message}");
            OnError?.Invoke(ex);
        }
    }

    /// <summary>
    /// 发送普通请求，并等待响应
    /// </summary>
    public async Task<string?> SendRequestAsync(string content, CancellationToken ct = default)
    {
        if (_disposed || _stream == null) return null;

        int msgId = GetNextMsgId();
        byte[] packet = MakeNormalRequest(msgId, content);

        var pending = new PendingMessage
        {
            MsgId = msgId,
            SendTime = DateTime.Now,
            RetryCount = 0,
            IsConfirmed = false
        };
        _pendingMessages[msgId] = pending;

        await _streamWriteLock.WaitAsync();
        try
        {
            await _stream.WriteAsync(packet, 0, packet.Length);
            await _stream.FlushAsync();
        }
        finally
        {
            _streamWriteLock.Release();
        }

        bool timedOut = !pending.ConfirmEvent.Wait(RESPONSE_TIMEOUT_MS, ct);
        if (timedOut)
        {
            Console.WriteLine($"[AIUIService] 请求 MsgId={msgId} 等待响应超时");
            _pendingMessages.TryRemove(msgId, out _);
            return null;
        }

        return pending.ResponseContent;
    }

    #endregion

    #region 协议编解码

    private Message ReadFrame(NetworkStream stream)
    {
        try
        {
            byte[] meta = ReadExactly(stream, 7);

            byte header = meta[0];
            byte userId = meta[1];
            byte msgType = meta[2];
            int msgLen = FromLittleEndian(meta.Skip(3).Take(2).ToArray());
            int msgId = FromLittleEndian(meta.Skip(5).Take(2).ToArray());

            byte[] msgContent = ReadExactly(stream, msgLen);
            int code = stream.ReadByte();

            if (!CheckCode(meta, msgContent, code))
            {
                Console.WriteLine("[AIUIService] 校验码不匹配！");
                return new Message();
            }

            return new Message(header, userId, msgType, msgLen, msgId, msgContent, code);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[AIUIService] 读取消息异常: {ex.Message}");
            return new Message();
        }
    }

    private byte[] ReadExactly(NetworkStream stream, int len)
    {
        byte[] data = new byte[len];
        int read = 0;
        while (read < len)
        {
            int r = stream.Read(data, read, len - read);
            if (r == 0) throw new Exception("read 0 socket closed");
            read += r;
        }
        return data;
    }

    private bool CheckCode(byte[] meta, byte[] msgContent, int code)
    {
        byte[] data = MergeArray(meta, msgContent);
        return CalCode(data) == code;
    }

    private int CalCode(byte[] data)
    {
        int sum = 0;
        foreach (byte b in data) sum += b;
        return (~sum + 1) & 0xFF;
    }

    private int GetNextMsgId()
    {
        lock (_msgIdLock)
        {
            _currentMsgId = (_currentMsgId + 1) % 65536;
            return _currentMsgId;
        }
    }

    private int GetNextTtsSid() => Interlocked.Increment(ref _ttsSidCounter);

    private int FromLittleEndian(byte[] data) => (data[1] << 8) | (data[0] & 0xFF);

    private byte[] ToLittleEndian(int value) => new[] { (byte)(value & 0xFF), (byte)((value >> 8) & 0xFF) };

    private byte[] MergeArray(byte[] first, byte[] second)
    {
        byte[] result = new byte[first.Length + second.Length];
        Buffer.BlockCopy(first, 0, result, 0, first.Length);
        Buffer.BlockCopy(second, 0, result, first.Length, second.Length);
        return result;
    }

    private byte[] MakeNormalRequest(int msgId, string content)
    {
        using var data = new MemoryStream();
        byte[] messageContent = Encoding.UTF8.GetBytes(content);
        data.WriteByte(0xA5);
        data.WriteByte(0x01);
        data.WriteByte(MSG_TYPE_NORMAL_REQUEST);
        data.Write(ToLittleEndian(messageContent.Length), 0, 2);
        data.Write(ToLittleEndian(msgId), 0, 2);
        data.Write(messageContent, 0, messageContent.Length);
        data.WriteByte((byte)CalCode(data.ToArray()));
        return data.ToArray();
    }

    private byte[] MakeTtsRequest(string text, int sid)
    {
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

        byte[] jsonBytes = Encoding.UTF8.GetBytes(jsonObject.ToString(Formatting.None));

        using var data = new MemoryStream();
        data.WriteByte(0xA5);
        data.WriteByte(0x01);
        data.WriteByte(MSG_TYPE_NORMAL_REQUEST);
        data.Write(ToLittleEndian(jsonBytes.Length), 0, 2);
        data.Write(ToLittleEndian(sid), 0, 2);
        data.Write(jsonBytes, 0, jsonBytes.Length);
        data.WriteByte((byte)CalCode(data.ToArray()));
        return data.ToArray();
    }

    private byte[] MakeConfirmMsg(int msgId)
    {
        using var data = new MemoryStream();
        byte[] messageContent = new byte[] { 0xA5, 0, 0, 0 };
        data.WriteByte(0xA5);
        data.WriteByte(0x01);
        data.WriteByte(MSG_TYPE_CONFIRM);
        data.Write(ToLittleEndian(messageContent.Length), 0, 2);
        data.Write(ToLittleEndian(msgId), 0, 2);
        data.Write(messageContent, 0, messageContent.Length);
        data.WriteByte((byte)CalCode(data.ToArray()));
        return data.ToArray();
    }

    #endregion

    #region 内部数据类

    private class PendingMessage
    {
        public int MsgId { get; set; }
        public DateTime SendTime { get; set; }
        public int RetryCount { get; set; }
        public bool IsConfirmed { get; set; }
        public string? ResponseContent { get; set; }
        public ManualResetEventSlim ConfirmEvent { get; set; } = new(false);
    }

    private class Message
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

    private static class CommonUtil
    {
        public static byte[] GZipDecompress(byte[] data)
        {
            if (data == null || data.Length == 0) return data ?? Array.Empty<byte>();
            using var compressedStream = new MemoryStream(data);
            using var gzipStream = new GZipStream(compressedStream, CompressionMode.Decompress);
            using var resultStream = new MemoryStream();
            gzipStream.CopyTo(resultStream);
            return resultStream.ToArray();
        }
    }

    #endregion
}
