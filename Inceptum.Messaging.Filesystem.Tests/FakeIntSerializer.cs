using System;

namespace Inceptum.Messaging.Filesystem.Tests
{
    internal class FakeIntSerializer : IMessageSerializer<int>
    {
        #region IMessageSerializer<int> Members

        public byte[] Serialize(int message)
        {
            return BitConverter.GetBytes(message);
        }

        public int Deserialize(byte[] message)
        {
            return BitConverter.ToInt16(message, 0);
        }

        #endregion
    }
}