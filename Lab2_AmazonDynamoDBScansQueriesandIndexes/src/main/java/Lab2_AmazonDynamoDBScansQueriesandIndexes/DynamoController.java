package Lab2_AmazonDynamoDBScansQueriesandIndexes;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Delete;
import io.micronaut.http.annotation.Get;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

@Controller("/dynamodb")
public class DynamoController {

  @Inject
  DynamoRepository dynamoRepository;
  @Inject
  MoviesRepository moviesRepository;

  @Delete(value = "/table/{tableName}", produces = MediaType.TEXT_PLAIN)
  public String deleteTable(String tableName) {
    return dynamoRepository.removeTable(tableName).toString();
  }

  @Get(value = "/table/{tableName}", produces = MediaType.TEXT_PLAIN)
  public String checkStatus(String tableName) {
    long startTime = System.currentTimeMillis();
    DescribeTableResponse response = dynamoRepository.checkStatus(tableName);
    String tableStatus = response.table().tableStatus().toString();

    long endTime = System.currentTimeMillis();
    double totalTime = (endTime - startTime) / 1000.0;
    return "Status of table is: " + tableStatus + "\n" + "Total time: " + totalTime + " sec";

  }

}