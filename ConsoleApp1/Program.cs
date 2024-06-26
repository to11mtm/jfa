// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using ConsoleApp1;
using LanguageExt;
using LanguageExt.Pretty;
using LinqToDB;
using LinqToDB.Data;
using MessagePack;
using NatsSandBox.Client;
using NatsSandBox.Client.JetStream;

//var d = new Wat();

internal class Program
{
    public static async Task Main(string[] args)
    {
        await NatsSandBox.Client.JetStream.Program.Main(args);
    }
}
