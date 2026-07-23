using AUUI;

namespace AUUI;

internal class Program
{
    public static async Task Main(string[] args)
    {
        string ip = "192.168.31.90";
        int port = 19199;

        using var service = new AIUIService(ip, port);

        // 订阅事件
        service.OnIatFinalResult += text =>
        {
            Console.WriteLine($"\n>>> 最终识别结果: {text}\n");
            // 示例：收到识别结果后自动回复 TTS
            _ = service.SendTtsAsync("好的，我已经收到了。");
        };

        service.OnIatPartialResult += text =>
        {
            Console.WriteLine($">>> 识别中间结果: {text}");
        };

        service.OnWakeUp += info =>
        {
            Console.WriteLine($"\n>>> 唤醒事件: {info}\n");
        };

        service.OnSemanticResult += json =>
        {
            Console.WriteLine($"\n>>> 语义结果: {json.ToString(Newtonsoft.Json.Formatting.None)}\n");
        };

        service.OnDeviceStatus += status =>
        {
            Console.WriteLine($">>> 设备状态: {status}");
        };

        service.OnAiuiError += err =>
        {
            Console.WriteLine($">>> AIUI 错误: {err}");
        };

        service.OnError += ex =>
        {
            Console.WriteLine($">>> 服务异常: {ex.Message}");
        };

        try
        {
            await service.StartAsync();
            Console.WriteLine("AIUI 服务已启动，按 Ctrl+C 退出...");

            // 保持运行
            await Task.Delay(Timeout.Infinite);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("服务已停止。");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"启动失败: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
    }
}
