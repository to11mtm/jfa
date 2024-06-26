using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using Akka.Actor;

namespace Akka.Remote.Transporrt.WS
{
    public class SocketActor : ActorBase
    {
        protected override bool Receive(object message)
        {
            Socket s;
            s.ConnectAsync((EndPoint)null, default).GetAwaiter().OnCompleted();
        }

        protected override bool AroundReceive(Receive receive, object message)
        {
            return base.AroundReceive(receive, message);
        }
    }

    public record CreateSocket
    {
        
    }
}