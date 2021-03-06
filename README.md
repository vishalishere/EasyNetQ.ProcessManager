An Easy Process Management API for [EasyNetQ](http://easynetq.com/)

Initial development was sponsered by travel industry experts [15below](http://www.15below.com/)

To create a step in a process, using information from previous steps:

``` csharp
public static Out SendAnEmail(RenderComplete rc, IState state)
{
	var ws = state.Get<WorkflowState>().Value;
	var sendEmailMessage = new SendEmail(Guid.NewGuid(), ws.EmailAddress, ws.EmailContent);
	return
		Out.Empty
		   // This message will be sent out via EasyNetQ
		   .Send(sendEmailMessage, TimeSpan.FromMinutes(5))
		   // This is a message we expect to recieve in the future,
		   // and which handler should process it
		   .Expect<EmailSent>(
				sendEmailMessage.CorrelationId.ToString(),
				"EmailSentHandler", TimeSpan.FromMinutes(6));
}
```

To add a managed handler (single subscription per type):

``` csharp
var rabbitConnString = ConfigurationManager.AppSettings["rabbit connection"];
var sqlConnString = ConfigurationManager.AppSettings["sql connection"];
var bus = RabbitHutch.CreateBus(rabbitConnString);
var active = new SqlActiveStore(sqlConnString);
var store = new SqlStateStore(sqlConnString, new Serializer());
var pm = new ProcessManager(new EasyNetQPMBus(bus), "Process", active, store);

// Add as many of these as needed, the ProcessManager will ensure
// one connection per Rabbit queue and that only the right handlers
// are called for the workflows you've created
pm.AddProcessor(sent => sent.CorrelationId.ToString(), new Mapping<EmailSent>("EmailSent", EmailSent));
```

A blog post to get you going: http://blog.mavnn.co.uk/easynetq-process-management/

Running build.bat will build and package the two core libraries for you. To run and
build the full solution with the example projects, you will need an SQL Server (or Express)
instance set up with the SQL files from the Messenger.Store and EasyNetQ.ProcessManager.State.SQLServer
projects. You can change the SQL Server, RabbitMQ and SMTP connection strings in the example_context/shared.app.config file.

## FAQ

Q. Aaaargh! I opened the source file and all my curly braces dissappeared! What's going on?

A. So, yeah. The code is written in F# - but with C#/VB.net usage in mind. Check out example Process3 for a good example of what consuming the code from C# looks like. And if you're interested in helping hack on the project but don't know F#? Don't worry, we're a friendly bunch. Just start asking questions in the Issues area and we'll help you out.
