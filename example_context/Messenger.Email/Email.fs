module Messenger.Email

open System.Net.Mail
open Messenger.Messages.Email

let send (smtp : SmtpClient) sendEmail =
    use message = new MailMessage("me@example.com", sendEmail.EmailAddress, "subject", sendEmail.Content)
    try
        smtp.Send message
        { CorrelationId = sendEmail.CorrelationId; Successful = true }
    with
    | _ -> { CorrelationId = sendEmail.CorrelationId; Successful = false}