using LanguageExt;
using static GlutenFree.Composition.TriState;
using JsonConverter = System.Text.Json.Serialization.JsonConverter;

namespace GlutenFree.Composition;

public class OptionOrError
{
    public static OptionOrError<T> Of<T>(T value) =>
        new OptionOrError<T>(value);

    public static OptionOrError<T> Error<T>(Exception error)
        => new OptionOrError<T>(error);
    public static OptionOrError<T> None<T>() => new OptionOrError<T>();
}
public readonly struct OptionOrError<T>
{
    private readonly Exception? _stateOrError;
    private readonly T? _value;
    
    public OptionOrError()
    {
        _stateOrError = default!;
        _value = default!;
    }

    public OptionOrError(Exception error)
    {
        _stateOrError = error;
        _value = default!;
    }

    public OptionOrError(T value)
    {
        _stateOrError = TriStateCompletedException.Instance;
        _value = value;
    }

    public TriStateResultEnum State =>
        _stateOrError == null ? TriStateResultEnum.NoVal
            : object.ReferenceEquals(_stateOrError,TriStateCompletedException.Instance)
                ? TriStateResultEnum.Value
                : TriStateResultEnum.Error;

    // ReSharper disable once ConvertToAutoProperty
    public T? Value => _value!;
}