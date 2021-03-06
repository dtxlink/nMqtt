﻿namespace DotNetty.Codecs.MqttFx.Packets
{
    /// <summary>
    /// 发布回执
    /// QoS level = 1
    /// </summary>
    public sealed class PubAckPacket : PacketWithIdentifier
    {
        public PubAckPacket(ushort packetId = default) 
            : base(PacketType.PUBACK)
        {
            VariableHeader.PacketIdentifier = packetId;
        }
    }
}
