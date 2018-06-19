import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class CapacityUp {
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    static DynamoDB dynamoDB = new DynamoDB(client);
    static int i = 1;
    static String tableName = "java-table3";
    public static void main(String[] args) throws IOException, InterruptedException {

        Table table = dynamoDB.getTable(tableName);

        final AmazonCloudWatch cloudWatch =
            AmazonCloudWatchClientBuilder.defaultClient();
    long offsetInMilliseconds = 100000;
    while(true) {
        Dimension instanceDimension = new Dimension();
        instanceDimension.setName("TableName");
        instanceDimension.setValue("java-table3");
        GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
                .withStartTime(new Date(new Date().getTime() - offsetInMilliseconds))
                .withNamespace("AWS/DynamoDB")
                .withPeriod(60)
                .withMetricName("WriteThrottleEvents")
                .withStatistics("Sum")
                .withDimensions(Arrays.asList(instanceDimension))
                .withEndTime(new Date());


        GetMetricStatisticsResult getMetricStatisticsResult = cloudWatch.getMetricStatistics(request);
        Double writeCapUnit = table.describe().getProvisionedThroughput().getWriteCapacityUnits().doubleValue();
        System.out.println("Current provisioned : " + writeCapUnit);
        if (getMetricStatisticsResult.getDatapoints().size() != 0) {
            Double currentSum = getMetricStatisticsResult.getDatapoints().get(0).getSum();
            int vol = 0;
            if (currentSum >= 22 && currentSum<=39) {
                System.out.println("Capacity increased by 2");
                vol=2;
            }
            else if(currentSum >= 40 && currentSum<=90)
            {
                System.out.println("Capacity increased by 5");
                vol=5;
            }
            else if(currentSum >=91)
            {
                System.out.println("Capacity increased by 10");
                vol=10;

            }
                try {
                    table.updateTable(new ProvisionedThroughput((long) 1, writeCapUnit.longValue() + vol));
                    System.out.println("Done");
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }



            ////////////////////Decrease
            //decrease - by sum consumedwrite




        Thread.sleep(300000);
        }

    }
}

