using System;
using System.Net.Sockets;
using System.Net.WebSockets;
using Akka.Actor;
using Akka.Remote.Transport;

namespace Akka.Remote.Transporrt.WS
{
    public class EffectiveActor : ActorBase
    {
        public interface ICommandChainAction<TIn, TOut>
        {
       //     public 
        }
        public class CommandChain
        {
            public CommandChain()
            {
                
            }
        }
        [Flags]
        public enum EffectState
        {
            None = 0,
            SystemStashing = 1
        }
        private class BeginSystemStashing
        {
            public static readonly BeginSystemStashing Instance = new();
        }

        private class EndSystemStashing
        {
            public static readonly EndSystemStashing Instance = new();
        }

        private class BeginAsyncSuspend
        {
            public static readonly BeginAsyncSuspend Instance = new();
        }

        private class EndAsyncSuspendSuccess
        {
            
        }
        protected EffectState CurrentEffectState { get; private set; }
        protected override bool Receive(object message)
        {
            if ((CurrentEffectState | EffectState.SystemStashing) > 0)
            {
                //System stashing; anything noncrit is stashed.
            }

            if (message is EndSystemStashing)
            {
                CurrentEffectState =
                    (CurrentEffectState & ~EffectState.SystemStashing);
            }
            else if (message is BeginSystemStashing)
            {
                CurrentEffectState =
                    CurrentEffectState | EffectState.SystemStashing;
            }
        }
    }
    public class WebSocketHandler
    {
        public WebSocketHandler()
        {
            
        }

        public void Connect()
        {
            var tc = new TcpClient();
            tc.GetStream();
        }
    }
    public class Class1 : IHandleEventListener
    {
        public void Notify(IHandleEvent ev)
        {
            throw new NotImplementedException();
        }
    }
}