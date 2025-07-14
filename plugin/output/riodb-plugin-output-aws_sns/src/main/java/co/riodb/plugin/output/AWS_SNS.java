/*-
 * Copyright (c) 2025 Lucio D Matos - www.riodb.co
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.riodb.plugin.output;

import co.riodb.plugin.resources.LoggingService;
import co.riodb.plugin.resources.PluginUtils;
import co.riodb.plugin.resources.RioDBOutputPlugin;
import co.riodb.plugin.resources.RioDBPluginException;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.SnsException;

public class AWS_SNS extends RioDBOutputPlugin {

    // if an error occurs (like invalid number)
    // we only log it once.
    private AtomicBoolean errorAlreadyCaught;

    private AtomicBoolean running;
    // AWS variables
    private AwsCredentialsProvider awsCredsProvider;

    private Region region;
    private String topicArn;

    public AWS_SNS(LoggingService loggingService) {
        super(loggingService);
    }

    public void clearWarnings() {
        super.clearWarnings();
        if (errorAlreadyCaught != null) {
            errorAlreadyCaught.set(false);
        }
    }

    @Override
    public int getQueueSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    public void init(String outputParams) throws RioDBPluginException {

        running = new AtomicBoolean(false);
        errorAlreadyCaught = new AtomicBoolean(false);

        logDebug("Initializing with paramters ( " + outputParams + ") ");

        // setup AWS SNS variables.
        String topicArnStr = PluginUtils.getParameter(outputParams, "aws_topic_arn");
        if (topicArnStr == null || topicArnStr.length() < 30) {
            throw new RioDBPluginException(this.getType() + " 'aws_topic_arn' parameter is required.");
        }
        this.topicArn = topicArnStr;

        // setup AWS SNS variables.
        String regionStr = PluginUtils.getParameter(outputParams, "aws_region");
        if (regionStr == null || regionStr.length() < 5) {
            throw new RioDBPluginException(
                    this.getType() + " 'aws_region' parameter is required. Example, 'us-west-1'");
        }
        regionStr = regionStr.toLowerCase();
        this.region = Region.of(regionStr);

        if (PluginUtils.hasParameter(outputParams, "aws_key") && PluginUtils.hasParameter(outputParams, "aws_secret")) {
            String keyStr = PluginUtils.getParameter(outputParams, "aws_key");
            String secretStr = PluginUtils.getParameter(outputParams, "aws_secret");

            logDebug("  Using user-provided aws_key and aws_secret.");

            AwsCredentials credentials = AwsBasicCredentials.create(keyStr, secretStr);

            this.awsCredsProvider = StaticCredentialsProvider.create(credentials);

        } else {

            logDebug(" Using default credentials provider. https:/" +
                    "/sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html");
            this.awsCredsProvider = DefaultCredentialsProvider.create();

        }

        logDebug("Initialized.");

    }

    private void publishToTopic(String message) {

        try {

            SnsClient snsClient = SnsClient.builder().credentialsProvider(awsCredsProvider).region(region).build();
            PublishRequest request = PublishRequest.builder()
                    .message(message)
                    .topicArn(topicArn)
                    .build();

            snsClient.publish(request);

            logDebug("Sent message");

        } catch (SnsException | software.amazon.awssdk.core.exception.SdkClientException e) {
            if (!errorAlreadyCaught.get()) {
                errorAlreadyCaught.set(true);
                logDebug(e.getMessage());
            }
        }
    }

    // method for invoking output
    public void sendOutput(String message) {

        if (running.get()) {
            // execute push from other thread
            new Thread(() -> {
                publishToTopic(message);
            }, "OUTPUT_SNS_POST").start();

        }

    }

    public void start() {

        running.set(true);
        logDebug("started.");

    }

    public void stop() {
        running.set(false);
        logDebug("Stopped.");
    }

}
