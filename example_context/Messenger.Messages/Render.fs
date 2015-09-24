namespace Messenger.Messages.Render
open System

type RequestRender =
    {
        CorrelationId : Guid
        TemplateId : int
        ModelId : int
    }

type RenderComplete =
    {
        CorrelationId : Guid
        Content : string
    }

