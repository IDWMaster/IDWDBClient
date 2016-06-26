/*
 This file is part of IDWDB Client Driver.
    IDWDB Client Driver is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    IDWDB Client Driver is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with IDWDB Client Driver.  If not, see <http://www.gnu.org/licenses/>.
 * */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IDWNatives
{
    public delegate void OnIOComplete(int bytesProcessed);
    [StructLayout(LayoutKind.Sequential)]
    struct Overlapped
    {
        public IntPtr Internal;
        public IntPtr InternalHigh;
        public int Offset;
        public int OffsetHigh;
        public IntPtr hEvent;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct WSABuf
    {

        public uint len;
        public IntPtr buf;
    }
    class AsyncIOOperation
    {
        public GCHandle bytes;
        public GCHandle overlapped;
        public GCHandle buffy;
        public IntPtr fd;
        public OnIOComplete cb;
        public GCHandle flags;
    }
    class IOLoop
    {
        public static IOLoop dispatch_loop = new IOLoop();
        List<AsyncIOOperation> ops = new List<AsyncIOOperation>();
        [DllImport("kernel32.dll")]
        [SuppressUnmanagedCodeSecurity]
        static extern void WaitForMultipleObjects(int numHandles, IntPtr[] handles, bool waitAll, int timeout);
        AutoResetEvent evt = new AutoResetEvent(false);
        public IOLoop()
        {
            Thread mthread = new Thread(delegate () {
                unsafe
                {
                    while (true)
                    {
                        IntPtr[] handles = null;
                        int count = -1;
                        lock (ops)
                        {
                            count = ops.Count;
                            handles = new IntPtr[count + 1];
                            fixed (IntPtr* handles_ptr = handles)
                            {

                                IntPtr* hptr = handles_ptr + 1;
                                handles[0] = evt.SafeWaitHandle.DangerousGetHandle();
                                for (int i = 0; i < ops.Count; i++)
                                {
                                    hptr[i] = ((Overlapped*)(ops[i].overlapped.AddrOfPinnedObject()))->hEvent;
                                }
                            }
                        }

                        WaitForMultipleObjects(count + 1, handles, false, -1);
                        lock (ops)
                        {
                            List<AsyncIOOperation> toRemove = new List<AsyncIOOperation>();
                            foreach (var iable in ops.ToList()) //Copy list in case callback updates it.
                            {
                                //!= 259
                                Overlapped* olap = (Overlapped*)iable.overlapped.AddrOfPinnedObject();
                                if (olap->Internal.ToInt64() != 259)
                                {
                                    int transferred = olap->InternalHigh.ToInt32();
                                    iable.cb(transferred);
                                    toRemove.Add(iable);
                                    iable.buffy.Free();
                                    iable.bytes.Free();
                                    iable.flags.Free();
                                    iable.overlapped.Free();
                                }
                            }
                            foreach (var iable in toRemove)
                            {
                                ops.Remove(iable);
                            }
                        }

                    }
                }
            });
            mthread.IsBackground = true;
            mthread.Start();
        }
        public void Post(AsyncIOOperation op)
        {
            lock (ops)
            {
                ops.Add(op);
                evt.Set();
            }
        }
    }
    public class FastNetworkStream
    {
        public FastNetworkStream(ulong handle)
        {
            this.Handle = new IntPtr((long)handle);
        }
        [SuppressUnmanagedCodeSecurity]
        [DllImport("Ws2_32.dll")]
        static extern int WSARecv(IntPtr socket, IntPtr buffers, uint bufferCount, IntPtr numberOfBytesReceivedPtr, IntPtr flags, IntPtr overlapped, IntPtr completionRoutine);
        [SuppressUnmanagedCodeSecurity]
        [DllImport("Ws2_32.dll")]
        static extern int WSAGetLastError();
        [SuppressUnmanagedCodeSecurity]
        [DllImport("Ws2_32.dll")]
        static extern int WSASend(IntPtr socket, IntPtr buffers, uint bufferCount, IntPtr numberOfBytesSentPtr, uint flags, IntPtr overlapped, IntPtr completionRoutine);
        [SuppressUnmanagedCodeSecurity]
        [DllImport("Kernel32.dll")]
        static extern IntPtr CreateEventW(IntPtr securityAttributes, bool isManualReset, bool initialState, IntPtr name);
        public IntPtr Handle;
        public void SendAsync(byte[] buffer, int offset, int count, OnIOComplete completionHandler)
        {
            unsafe
            {
                AsyncIOOperation op = new AsyncIOOperation();
                op.overlapped = GCHandle.Alloc(new Overlapped(), GCHandleType.Pinned);
                Overlapped* overlapped = (Overlapped*)op.overlapped.AddrOfPinnedObject();
                overlapped->hEvent = CreateEventW(IntPtr.Zero, true, false, IntPtr.Zero);
                op.buffy = GCHandle.Alloc(new WSABuf(), GCHandleType.Pinned);
                WSABuf* buffy = (WSABuf*)op.buffy.AddrOfPinnedObject();
                op.bytes = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                buffy->buf = op.bytes.AddrOfPinnedObject() + offset;
                buffy->len = (uint)count;
                op.flags = GCHandle.Alloc(0, GCHandleType.Pinned);
                int r = WSASend(Handle, op.buffy.AddrOfPinnedObject(), 1, IntPtr.Zero, 0, new IntPtr(overlapped), IntPtr.Zero);
                int a = WSAGetLastError();
                op.fd = Handle;
                op.cb = completionHandler;
                IOLoop.dispatch_loop.Post(op);
            }
        }
        public void ReceiveAsync(byte[] buffer, int offset, int count, OnIOComplete completionHandler)
        {
            unsafe
            {
                AsyncIOOperation op = new AsyncIOOperation();
                op.overlapped = GCHandle.Alloc(new Overlapped(), GCHandleType.Pinned);
                Overlapped* overlapped = (Overlapped*)op.overlapped.AddrOfPinnedObject();
                overlapped->hEvent = CreateEventW(IntPtr.Zero, true, false, IntPtr.Zero);
                op.buffy = GCHandle.Alloc(new WSABuf(), GCHandleType.Pinned);
                WSABuf* buffy = (WSABuf*)op.buffy.AddrOfPinnedObject();
                op.bytes = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                buffy->buf = op.bytes.AddrOfPinnedObject() + offset;
                buffy->len = (uint)count;
                op.flags = GCHandle.Alloc(0, GCHandleType.Pinned);
                int r = WSARecv(Handle, new IntPtr(buffy), 1, IntPtr.Zero, op.flags.AddrOfPinnedObject(), new IntPtr(overlapped), IntPtr.Zero);
                int a = WSAGetLastError();
                op.fd = Handle;
                op.cb = completionHandler;
                IOLoop.dispatch_loop.Post(op);
            }
        }
    }
}
