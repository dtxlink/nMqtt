using DotNetty.Codecs.MqttFx.Packets;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MqttFx.Channels;
using MqttFx.Utils;
using System;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace MqttFx.Client
{
    /// <summary>
    /// Mqtt客户端
    /// </summary>
    public class MqttClient
    {
        private readonly ILogger _logger;
        private IEventLoopGroup _eventLoop;
        private volatile IChannel _channel;
        private readonly PacketIdProvider _packetIdProvider = new PacketIdProvider();
        private readonly PacketDispatcher _packetDispatcher = new PacketDispatcher();

        private Socket clientSocket;

        public bool IsConnected { get; private set; }

        public MqttClientOptions Options { get; }

        public IMqttClientConnectedHandler ConnectedHandler { get; set; }

        public IMessageReceivedHandler MessageReceivedHandler { get; set; }

        public IMqttClientDisconnectedHandler DisconnectedHandler { get; set; }

        public MqttClient(ILogger<MqttClient> logger, IOptions<MqttClientOptions> options)
        {
            _logger = logger ?? NullLogger<MqttClient>.Instance;
            Options = options.Value;
        }

        /// <summary>
        /// 连接
        /// </summary>
        /// <returns></returns>
        public async ValueTask<bool> ConnectAsync(EndPoint remoteEndPoint, CancellationToken cancellationToken)
        {
            clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                await clientSocket.ConnectAsync(remoteEndPoint);

                await Task.Factory.StartNew(async (state) =>
                {
                    var session = (Socket)state;
                    if (_logger.IsEnabled(LogLevel.Information))
                    {
                        _logger.LogInformation($"[Connected]:{session.LocalEndPoint} to {session.RemoteEndPoint}");
                    }
                    var pipe = new Pipe();
                    Task writing = FillPipeAsync(session, pipe.Writer, cancellationToken);
                    Task reading = ReadPipeAsync(session, pipe.Reader);
                    await Task.WhenAll(reading, writing);
                }, clientSocket);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
                throw new MqttException("BrokerUnavailable: " + ex.Message);
            }
            return true;
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="payload">有效载荷</param>
        /// <param name="qos">服务质量等级</param>
        /// <param name="retain"></param>
        public Task PublishAsync(string topic, byte[] payload, MqttQos qos, bool retain, CancellationToken cancellationToken)
        {
            var packet = new PublishPacket(qos, false, retain)
            {
                TopicName = topic,
                Payload = payload
            };
            if (qos > MqttQos.AtMostOnce)
                packet.PacketIdentifier = _packetIdProvider.NewPacketId();

            return SendPacketAsync(packet);
        }

        /// <summary>
        /// 订阅主题
        /// </summary>
        /// <param name="topic">主题</param>
        /// <param name="qos">服务质量等级</param>
        /// <param name="cancellationToken"></param>
        public Task SubscribeAsync(string topic, MqttQos qos, CancellationToken cancellationToken)
        {
            var packet = new SubscribePacket();
            packet.VariableHeader.PacketIdentifier = _packetIdProvider.NewPacketId();
            packet.Add(topic, qos);

            return SendPacketAsync(packet);
        }

        /// <summary>
        /// 取消订阅
        /// </summary>
        /// <param name="topics">主题</param>
        public Task UnsubscribeAsync(params string[] topics)
        {
            var packet = new UnsubscribePacket();
            packet.AddRange(topics);

            return SendPacketAsync(packet);
        }

        /// <summary>
        /// 断开连接
        /// </summary>
        /// <returns></returns>
        public async Task DisconnectAsync()
        {
            if (_channel != null)
                await _channel.CloseAsync();

            await _eventLoop.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
        }

        /// <summary>
        /// 发送包
        /// </summary>
        /// <param name="packet"></param>
        /// <returns></returns>
        private Task SendPacketAsync(Packet packet)
        {
            if (_channel == null)
                return Task.CompletedTask;

            if (_channel.Active)
                return _channel.WriteAndFlushAsync(packet);

            return Task.CompletedTask;
        }

        private async Task FillPipeAsync(Socket session, PipeWriter writer, CancellationToken cancellationToken)
        {
            while (true)
            {
                try
                {
                    Memory<byte> memory = writer.GetMemory(8096);
                    int bytesRead = await session.ReceiveAsync(memory, SocketFlags.None, cancellationToken);
                    if (bytesRead == 0)
                    {
                        break;
                    }
                    writer.Advance(bytesRead);
                }
                catch (SocketException ex)
                {
                    _logger.LogError($"[{ex.SocketErrorCode},{ex.Message}]:{session.RemoteEndPoint}");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"[Receive Error]:{session.RemoteEndPoint}");
                    break;
                }
                FlushResult result = await writer.FlushAsync();
                if (result.IsCompleted)
                {
                    break;
                }
            }
            writer.Complete();
        }

        private async Task ReadPipeAsync(Socket session, PipeReader reader)
        {
            while (true)
            {
                ReadResult result = await reader.ReadAsync();
                if (result.IsCompleted)
                {
                    break;
                }
                ReadOnlySequence<byte> buffer = result.Buffer;
                SequencePosition consumed = buffer.Start;
                SequencePosition examined = buffer.End;
                try
                {
                    if (result.IsCanceled) break;
                    if (buffer.Length > 0)
                    {
                        ReaderBuffer(ref buffer, session, out consumed, out examined);
                    }
                }
                catch (Exception ex)
                {
                    //Close();
                    break;
                }
                finally
                {
                    reader.AdvanceTo(consumed, examined);
                }
            }
            reader.Complete();
        }

        private void ReaderBuffer(ref ReadOnlySequence<byte> buffer, Socket session, out SequencePosition consumed, out SequencePosition examined)
        {
            consumed = buffer.Start;
            examined = buffer.End;
            SequenceReader<byte> seqReader = new SequenceReader<byte>(buffer);
            if (seqReader.TryPeek(out byte beginMark))
            {
                //if (beginMark != JT808Package.BeginFlag) throw new ArgumentException("Not JT808 Packages.");
            }
            byte mark = 0;
            long totalConsumed = 0;
            while (!seqReader.End)
            {
                //if (seqReader.IsNext(JT808Package.BeginFlag, advancePast: true))
                //{
                //    if (mark == 1)
                //    {
                //        try
                //        {
                //            var package = JT808Serializer.HeaderDeserialize(seqReader.Sequence.Slice(totalConsumed, seqReader.Consumed - totalConsumed).ToArray(), minBufferSize: 8096);
                //            ReceiveAtomicCounterService.MsgSuccessIncrement();
                //            if (Logger.IsEnabled(LogLevel.Debug)) Logger.LogDebug($"[Atomic Success Counter]:{ReceiveAtomicCounterService.MsgSuccessCount}");
                //            if (Logger.IsEnabled(LogLevel.Trace)) Logger.LogTrace($"[Accept Hex {session.RemoteEndPoint}]:{package.OriginalData.ToArray().ToHexString()}");
                //        }
                //        catch (JT808Exception ex)
                //        {
                //            Logger.LogError(ex, $"[HeaderDeserialize ErrorCode]:{ ex.ErrorCode}");
                //        }
                //        totalConsumed += (seqReader.Consumed - totalConsumed);
                //        if (seqReader.End) break;
                //        seqReader.Advance(1);
                //        mark = 0;
                //    }
                //    mark++;
                //}
                //else
                //{
                //    seqReader.Advance(1);
                //}
            }
            if (seqReader.Length == totalConsumed)
            {
                examined = consumed = buffer.End;
            }
            else
            {
                consumed = buffer.GetPosition(totalConsumed);
            }
        }
    }
}
