package pers.minamoto54.aws.kinesis;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.AbstractWriterAppender;
import org.apache.logging.log4j.core.appender.WriterManager;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import pers.minamoto54.aws.kinesis.FirehoseManagerFactory.InitParameters;

@Plugin(name = "Firehose", category = "Core", elementType = "appender", printObject = true)
public class FirehoseAppender extends AbstractWriterAppender<WriterManager> {

    private static final FirehoseManagerFactory MANAGER_FACTORY = new FirehoseManagerFactory();

    @PluginFactory
    public static FirehoseAppender createAppender(
                    @PluginAttribute("name") final String name,
                    @PluginAttribute("deliveryStreamName") final String deliveryStreamName,
                    @PluginAttribute("bufferSize") final int bufferSize,
                    @PluginAttribute("maxPutRecordDelay") final int maxPutRecordDelay,
                    @PluginAttribute("region") final String region,
                    @PluginAttribute("maxRetries") final int maxRetries,
                    @PluginAttribute("encoding") final String encoding,
                    @PluginElement("Filters") final Filter filter,
                    @PluginElement("Layout") final StringLayout layout) {

        final InitParameters params = new InitParameters(name, deliveryStreamName, encoding, maxRetries, region, bufferSize, maxPutRecordDelay, layout);
        return new FirehoseAppender(name, filter, layout, WriterManager.getManager(deliveryStreamName, params, MANAGER_FACTORY));
    }

    private FirehoseAppender(final String name, final Filter filter, final StringLayout layout, final WriterManager manager) {
        super(name, layout, filter, true, false, manager);
    }
}
