package Lab2_AmazonDynamoDBScansQueriesandIndexes;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;

@Requires(beans = {DynamoRepository.class})
@Singleton
public class MoviesRepository {

  private final DynamoDbClient dynamoDbClient;

  @Inject
  public MoviesRepository(DynamoRepository dynamoRepository) {
    this.dynamoDbClient = dynamoRepository.getDynamoDbClient();
  }

  public CreateTableResponse createTable() {
    String tableName = "movies";
    CreateTableRequest request = CreateTableRequest.builder()
        .tableName(tableName)
        .keySchema(
            KeySchemaElement.builder().attributeName("year").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("title").keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("year").attributeType(ScalarAttributeType.N).build(),
            AttributeDefinition.builder().attributeName("title").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("genre").attributeType(ScalarAttributeType.S).build()
        )
        .localSecondaryIndexes(
            LocalSecondaryIndex.builder()
                .indexName("genre-index")
                .keySchema(
                    KeySchemaElement.builder().attributeName("year").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("genre").keyType(KeyType.RANGE).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
        )
        .provisionedThroughput(
            ProvisionedThroughput.builder()
                .readCapacityUnits(1000L)
                .writeCapacityUnits(1000L)
                .build()
        )
        .build();

    return dynamoDbClient.createTable(request);
  }

  public String bulkLoad(String datafile) {
    long startTime = System.currentTimeMillis();

    try {
      // Load data from the JSON file
      JSONParser parser = new JSONParser();
      JSONArray moviesJson = (JSONArray) parser.parse(new FileReader(datafile));
      List<Map<String, AttributeValue>> movies = new ArrayList<>();
      for (Object obj : moviesJson) {
        Map<String, AttributeValue> movie = new HashMap<>();
        Map<?, ?> movieData = (Map<?, ?>) obj;
        movieData.forEach((k, v) -> {
          if (v instanceof String) {
            movie.put((String) k, AttributeValue.builder().s((String) v).build());
          } else if (v instanceof Number) {
            movie.put((String) k, AttributeValue.builder().n(v.toString()).build());
          } else if (v instanceof List) {
            List<String> stringList = ((List<?>) v).stream().map(Object::toString).collect(Collectors.toList());
            movie.put((String) k, AttributeValue.builder().ss(stringList).build());
          }
        });
        movies.add(movie);
      }

      // Use multi-threading to insert data into DynamoDB
      ExecutorService executor = Executors.newFixedThreadPool(10);

      // Load movies from JSON
      for (int i = 0; i < movies.size(); i += 1100) {
        int end = Math.min(i + 1100, movies.size());
        List<Map<String, AttributeValue>> sublist = movies.subList(i, end);
        executor.execute(() -> loadMovies(sublist));
        Thread.sleep(2000);
      }

      // Generate and load additional movies
      for (int i = 0; i < 40000; i += 4000) {
        int end = i + 4000;
        List<Map<String, AttributeValue>> generatedMovies = new ArrayList<>();
        for (int j = i; j < end; j++) {
          generatedMovies.add(genMovie(j));
        }
        executor.execute(() -> loadMovies(generatedMovies));
        Thread.sleep(2000);
      }

      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (Exception e) {
      return "Error: " + e.getMessage();
    }

    long endTime = System.currentTimeMillis();
    double totalTime = (endTime - startTime) / 1000.0;

    return "Total time: " + totalTime + " sec";
  }

  public Map<String, Integer> moviesScanYG(int yearToFind, String genreToFind) {
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":yearVal", AttributeValue.builder().n(String.valueOf(yearToFind)).build());
    expressionAttributeValues.put(":genreVal", AttributeValue.builder().s(genreToFind).build());

    // Use '#yr' as a placeholder for the 'year' attribute name
    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#yr", "year");
    expressionAttributeNames.put("#gn", "genre");

    String filterExpression = "#yr = :yearVal AND contains(#gn, :genreVal)";

    ScanRequest scanRequest = ScanRequest.builder()
        .tableName("movies")
        .filterExpression(filterExpression)
        .expressionAttributeValues(expressionAttributeValues)
        .expressionAttributeNames(expressionAttributeNames)
        .build();

    ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

    int recordCount = scanResponse.count();
    int recordsScannedCount = scanResponse.scannedCount();

    while (scanResponse.lastEvaluatedKey() != null && !scanResponse.lastEvaluatedKey().isEmpty()) {
      scanRequest = scanRequest.toBuilder().exclusiveStartKey(scanResponse.lastEvaluatedKey()).build();
      scanResponse = dynamoDbClient.scan(scanRequest);
      recordCount += scanResponse.count();
      recordsScannedCount += scanResponse.scannedCount();
    }

    Map<String, Integer> result = new HashMap<>();
    result.put("recordCount", recordCount);
    result.put("recordsScannedCount", recordsScannedCount);

    return result;
  }

  public QueryResponse moviesQueryWithLsiYG(int yearToFind, String genreToFind) {
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":yearVal", AttributeValue.builder().n(String.valueOf(yearToFind)).build());
    expressionAttributeValues.put(":genreVal", AttributeValue.builder().s(genreToFind).build());

    // Use '#yr' as a placeholder for the 'year' attribute name
    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#yr", "year");

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName("movies")
        .indexName("genre-index")
        .keyConditionExpression("#yr = :yearVal AND genre = :genreVal")
        .expressionAttributeValues(expressionAttributeValues)
        .expressionAttributeNames(expressionAttributeNames)
        .build();

    return dynamoDbClient.query(queryRequest);
  }

  public QueryResponse moviesQueryYG(int yearToFind, String genreToFind) {
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":yearVal", AttributeValue.builder().n(String.valueOf(yearToFind)).build());
    expressionAttributeValues.put(":genreVal", AttributeValue.builder().s(genreToFind).build());

    // Use '#yr' as a placeholder for the 'year' attribute name
    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#yr", "year");

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName("movies")
        .keyConditionExpression("#yr = :yearVal")
        .filterExpression("genre = :genreVal")
        .expressionAttributeValues(expressionAttributeValues)
        .expressionAttributeNames(expressionAttributeNames)
        .build();

    return dynamoDbClient.query(queryRequest);
  }

  public UpdateTableResponse createGSI(String attributeName, String attributeType) {
    String newIndexName = attributeName + "-globo-index";

    UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
        .tableName("movies")
        .attributeDefinitions(AttributeDefinition.builder()
                                  .attributeName(attributeName)
                                  .attributeType(attributeType)
                                  .build())
        .globalSecondaryIndexUpdates(GlobalSecondaryIndexUpdate.builder()
                                         .create(CreateGlobalSecondaryIndexAction.builder()
                                                     .indexName(newIndexName)
                                                     .keySchema(KeySchemaElement.builder()
                                                                    .attributeName(attributeName)
                                                                    .keyType(KeyType.HASH)
                                                                    .build())
                                                     .projection(Projection.builder()
                                                                     .projectionType(ProjectionType.ALL)
                                                                     .build())
                                                     .provisionedThroughput(ProvisionedThroughput.builder()
                                                                                .readCapacityUnits(1000L)
                                                                                .writeCapacityUnits(1000L)
                                                                                .build()).build())
                                         .build())
        .build();

    return dynamoDbClient.updateTable(updateTableRequest);
  }

  public QueryResponse queryByGenre(String genreToFind) {
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    expressionAttributeValues.put(":genreVal", AttributeValue.builder().s(genreToFind).build());

    // Use '#g' as a placeholder for the 'genre' attribute name
    Map<String, String> expressionAttributeNames = new HashMap<>();
    expressionAttributeNames.put("#g", "genre");

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName("movies")
        .indexName("genre-globo-index")
        .keyConditionExpression("#g = :genreVal")
        .expressionAttributeValues(expressionAttributeValues)
        .expressionAttributeNames(expressionAttributeNames)
        .build();

    return dynamoDbClient.query(queryRequest);
  }


  private Map<String, AttributeValue> genMovie(int id) {
    int year = ThreadLocalRandom.current().nextInt(1920, 2018);
    String idStr = String.valueOf(id);
    String title = "Junkerdata" + year + idStr + " -- junk even harder";
    List<String> directors = Collections.singletonList("Horst Bleve" + idStr);
    String releaseDate = year + "-06-03T00:00:00Z";
    String imageUrl = "http://ia.media-imdb.com/images/M/MV5BMTQxODE3NjM1Ml5BMl5BanBnXkFtZTcwMzkzNjc4OA@@._V1_SX400_" + idStr + ".jpg";
    String plot = "In the year " + year + ", only more junk can save the database";
    String genre = List.of("FakeActionData", "FakeDramaData", "FakeHorrorData", "FakeFamilyData", "FakeSci-FiData", "FakeCrimeData", "FakeComedyData",
                           "FakeFakeData", "FakeGenreData", "FakeMoreData").get(ThreadLocalRandom.current().nextInt(0, 10));

    Map<String, AttributeValue> movie = new HashMap<>();
    movie.put("year", AttributeValue.builder().n(String.valueOf(year)).build());
    movie.put("title", AttributeValue.builder().s(title).build());
    movie.put("directors", AttributeValue.builder().ss(directors).build());
    movie.put("release_date", AttributeValue.builder().s(releaseDate).build());
    movie.put("image_url", AttributeValue.builder().s(imageUrl).build());
    movie.put("plot", AttributeValue.builder().s(plot).build());
    movie.put("genre", AttributeValue.builder().s(genre).build());

    // Add other attributes like rating, rank, running_time_secs, and actors as needed

    return movie;
  }

  private void loadMovies(List<Map<String, AttributeValue>> movies) {
    movies.forEach(movie -> dynamoDbClient.putItem(PutItemRequest.builder().tableName("movies").item(movie).build()));
  }
}
