namespace EasyNetQ.ProcessManager.State.SqlServer

open System

type ISerializer =
    abstract member CanSerialize : Type -> bool
    abstract member CanSerialize<'a> : unit -> bool
    abstract member Serialize<'a> : 'a -> string
    abstract member Deserialize<'a> : string -> 'a


