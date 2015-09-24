namespace Messenger.Messages.Store
open System
open System.Collections.Generic

type StoreTemplate =
    {
        CorrelationId : Guid
        Name : string
        Template : string
    }

type TemplateStored =
    {
        CorrelationId : Guid
        TemplateId : int
    }

type BroadcastTemplate =
    {
        CorrelationId : Guid
        TemplateId : int
    }

type TemplateBroadcasted =
    {
        CorrelationId : Guid
        TemplateId : int
        Name : string
        Template : string
    }

type StoreModel =
    {
        CorrelationId : Guid
        Model : IDictionary<string, obj>
    }

type ModelStored =
    {
        CorrelationId : Guid
        ModelId : int
    }

type BroadcastModel =
    {
        CorrelationId : Guid
        ModelId : int
    }

type ModelBroadcasted =
    {
        CorrelationId : Guid
        ModelId : int
        Model : IDictionary<string, obj>
    }