using Akka.Actor;

namespace GlutenFree.ServiceActor;

public interface IServiceActor<TSelf, TCtorArgs> where TSelf: ActorBase
{
    static abstract TSelf CreateFor(TCtorArgs args);
}

public record POCArgs(string Wat);
public class ServicePOCActor : ReceiveActor, IServiceActor<ServicePOCActor,POCArgs>
{
    public ServicePOCActor(POCArgs args)
    {
        
    }
    public static Props CreateFor(POCArgs args)
    {
        return Props.Create(typeof(ServicePOCActor),args);
    }
}