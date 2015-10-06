using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using EasyNetQ.ProcessManager;
using Messenger.Messages.Email;
using Messenger.Messages.Render;
using Messenger.Messages.Store;

namespace Process3
{
    [DataContract]
    public class WorkflowState
    {
        public WorkflowState() { }

        public WorkflowState(int? modelId, int? contentTemplateId, string emailContent, string emailAddress, int? addressTemplateId)
        {
            ModelId = modelId;
            ContentTemplateId = contentTemplateId;
            EmailContent = emailContent;
            EmailAddress = emailAddress;
            AddressTemplateId = addressTemplateId;
        }

        [DataMember]
        public int? ModelId { get; set; }
        [DataMember]
        public int? ContentTemplateId { get; set; }
        [DataMember]
        public int? AddressTemplateId { get; set; }
        [DataMember]
        public string EmailContent { get; set; }
        [DataMember]
        public string EmailAddress { get; set; }
    }

    public class Workflow
    {
        // Callback keys
        private const string ModelStoredCheckRenderContentKey = "ModelStoredCheckRenderContentKey";
        private const string ModelStoredCheckRenderAddressKey = "ModelStoredCheckRenderAddress";
        private const string ContentTemplateStoredCheckRenderContentKey = "ContentTemplateStoredCheckRenderContent";
        private const string AddressTemplateStoredCheckRenderAddressKey = "AddressTemplateStoredCheckRenderAddress";
        private const string ContentRenderedCheckSendEmailKey = "ContentRenderedCheckSendEmail";
        private const string AddressRenderedCheckSendEmailKey = "AddressRenderedCheckSendEmail";

        public static Out Start(IDictionary<string, object> model, string contentTemplate, string addressTemplate)
        {
            var modelCid = Guid.NewGuid();
            var contentCid = Guid.NewGuid();
            var addressCid = Guid.NewGuid();
            return
                Out.Empty
                    .AddRequest(new StoreModel(modelCid, model), TimeSpan.FromSeconds(4))
                    .AddCont<ModelStored>(modelCid.ToString(), ModelStoredCheckRenderContentKey, TimeSpan.FromSeconds(5), "TimeOut")
                    .AddCont<ModelStored>(modelCid.ToString(), ModelStoredCheckRenderAddressKey, TimeSpan.FromSeconds(5), "TimeOut")
                    .AddRequest(new StoreTemplate(contentCid, "content template", contentTemplate), TimeSpan.FromSeconds(4))
                    .AddCont<TemplateStored>(contentCid.ToString(), ContentTemplateStoredCheckRenderContentKey, TimeSpan.FromSeconds(5),
                        "TimeOut")
                    .AddRequest(new StoreTemplate(addressCid, "address template", addressTemplate), TimeSpan.FromSeconds(4))
                    .AddCont<TemplateStored>(addressCid.ToString(), AddressTemplateStoredCheckRenderAddressKey, TimeSpan.FromSeconds(5),
                        "TimeOut");
        }

        private static Out RenderContentIfReady(WorkflowState state)
        {
            if (!state.ModelId.HasValue || !state.ContentTemplateId.HasValue) return Out.Ignore;
            var cid = Guid.NewGuid();
            var renderContent =
                new RequestRender(cid, state.ContentTemplateId.Value, state.ModelId.Value);
            return
                Out.Empty
                    .AddRequest(renderContent, TimeSpan.FromSeconds(4))
                    .AddCont<RenderComplete>(cid.ToString(), ContentRenderedCheckSendEmailKey, TimeSpan.FromSeconds(5),
                        "TimeOut");
        }

        private static Out RenderAddressIfReady(WorkflowState state)
        {
            if (!state.ModelId.HasValue || !state.AddressTemplateId.HasValue) return Out.Ignore;
            var cid = Guid.NewGuid();
            var renderContent =
                new RequestRender(cid, state.AddressTemplateId.Value, state.ModelId.Value);
            return Out.Empty.AddRequest(renderContent, TimeSpan.FromSeconds(4))
                .AddCont<RenderComplete>(cid.ToString(), AddressRenderedCheckSendEmailKey, TimeSpan.FromSeconds(5),
                    "TimeOut");
        }

        public static Out ModelStoredCheckRenderContent(ModelStored ms, IState state)
        {
            var ws = state.AddOrUpdate(new WorkflowState {ModelId = ms.ModelId}, existing =>
            {
                existing.ModelId = ms.ModelId;
                return existing;
            });
            return RenderContentIfReady(ws);
        }

        public static Out ModelStoredCheckRenderAddress(ModelStored ms, IState state)
        {
            var ws = state.AddOrUpdate(new WorkflowState {ModelId = ms.ModelId}, existing =>
            {
                existing.ModelId = ms.ModelId;
                return existing;
            });
            return RenderAddressIfReady(ws);
        }

        public static Out ContentTemplateStoredCheckRenderContent(TemplateStored ts, IState state)
        {
            var ws = state.AddOrUpdate(new WorkflowState {ContentTemplateId = ts.TemplateId}, existing =>
            {
                existing.ContentTemplateId = ts.TemplateId;
                return existing;
            });
            return RenderContentIfReady(ws);
        }

        public static Out AddressTemplateStoredCheckRenderAddress(TemplateStored ts, IState state)
        {
            var ws = state.AddOrUpdate(new WorkflowState {AddressTemplateId = ts.TemplateId}, existing =>
            {
                existing.AddressTemplateId = ts.TemplateId;
                return existing;
            });
            return RenderAddressIfReady(ws);
        }

        private static Out SendEmailIfReady(WorkflowState state)
        {
            if (state.EmailAddress == null || state.EmailContent == null) return Out.Ignore;
            var cid = Guid.NewGuid();
            var sendEmail =
                new SendEmail(cid, state.EmailAddress, state.EmailContent);
            return Out.Empty.AddRequest(sendEmail, TimeSpan.FromSeconds(4))
                .AddCont<EmailSent>(cid.ToString(), "EmailSent", TimeSpan.FromSeconds(5), "TimeOut");
        }

        public static Out AddressRenderedCheckSendEmail(RenderComplete rc, IState state)
        {
            var ws = state.AddOrUpdate(new WorkflowState {EmailAddress = rc.Content}, existing =>
            {
                existing.EmailAddress = rc.Content;
                return existing;
            });
            return SendEmailIfReady(ws);
        }

        public static Out ContentRenderedCheckSendEmail(RenderComplete rc, IState state)
        {
            var ws = state.AddOrUpdate(new WorkflowState {EmailContent = rc.Content}, existing =>
            {
                existing.EmailContent = rc.Content;
                return existing;
            });
            return SendEmailIfReady(ws);
        }

        public static Out EmailSent(EmailSent es, IState state)
        {
            var ws = state.Get<WorkflowState>().Value;
            Console.WriteLine("Email send success: {0}\nAddress: {1}\nContent: {2}", es.Successful, ws.EmailAddress, ws.EmailContent);
            return Out.End;
        }

        public static Out TimeOut(TimeOutMessage to, IState state)
        {
            Console.WriteLine("Time out waiting for: {0}", to.TimedOutStep);
            return Out.End;
        }

        public static void Configure(ProcessManager pm)
        {
            pm.AddProcessor(stored => stored.CorrelationId.ToString(), new []
            {
                new Mapping<ModelStored>(ModelStoredCheckRenderContentKey, ModelStoredCheckRenderContent),
                new Mapping<ModelStored>(ModelStoredCheckRenderAddressKey, ModelStoredCheckRenderAddress)
            });

            pm.AddProcessor(stored => stored.CorrelationId.ToString(), new[]
            {
                new Mapping<TemplateStored>(ContentTemplateStoredCheckRenderContentKey,
                    ContentTemplateStoredCheckRenderContent),
                new Mapping<TemplateStored>(AddressTemplateStoredCheckRenderAddressKey,
                    AddressTemplateStoredCheckRenderAddress)
            });

            pm.AddProcessor(complete => complete.CorrelationId.ToString(), new []
            {
                new Mapping<RenderComplete>(AddressRenderedCheckSendEmailKey, AddressRenderedCheckSendEmail),
                new Mapping<RenderComplete>(ContentRenderedCheckSendEmailKey, ContentRenderedCheckSendEmail)
            });

            pm.AddProcessor(sent => sent.CorrelationId.ToString(), new Mapping<EmailSent>("EmailSent", EmailSent));

            pm.AddProcessor(to => to.CorrelationId.ToString(), new Mapping<TimeOutMessage>("TimeOut", TimeOut));
        }
    }
}
