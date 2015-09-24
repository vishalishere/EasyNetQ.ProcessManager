namespace Messenger.Messages.Email

open System

type SendEmail =
    {
        CorrelationId : Guid
        EmailAddress : string
        Content : string
    }

type EmailSent =
    {
        CorrelationId : Guid
        Successful : bool
    }