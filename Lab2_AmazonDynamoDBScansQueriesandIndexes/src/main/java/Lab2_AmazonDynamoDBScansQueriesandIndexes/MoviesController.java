package Lab2_AmazonDynamoDBScansQueriesandIndexes;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;

@Controller("/movies")
public class MoviesController {

  private final MoviesRepository moviesRepository;

  @Inject
  public MoviesController(MoviesRepository moviesRepository) {
    this.moviesRepository = moviesRepository;
  }

  @Post(value = "/create", produces = MediaType.APPLICATION_JSON)
  public String createTable() {
    CreateTableResponse table = moviesRepository.createTable();
    return table.toString();
  }

  @Get(value = "/bulkload", produces = MediaType.TEXT_PLAIN)
  public String bulkLoad(@QueryValue String datafile) {
    return moviesRepository.bulkLoad(datafile);
  }

  @Get(value = "/scanMovies", produces = MediaType.APPLICATION_JSON)
  public Map<String, Integer> scanMovies(Integer year, String genre) {
    return moviesRepository.moviesScanYG(year, genre);
  }

  @Get(value = "/moviesQueryWithLsiYG", produces = MediaType.APPLICATION_JSON)
  public Map<String, Object> queryMoviesWithGenre(Integer year, String genre) {
    long startTime = System.currentTimeMillis();

    QueryResponse response = moviesRepository.moviesQueryWithLsiYG(year, genre);

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    Map<String, Object> result = new HashMap<>();
    result.put("Count", response.count());
    result.put("ScannedCount", response.scannedCount());
    result.put("TotalTimeSeconds", duration / 1000.0);

    return result;
  }

  @Get(value = "/moviesQueryYG", produces = MediaType.APPLICATION_JSON)
  public Map<String, Object> queryMovies(Integer year, String genre) {
    long startTime = System.currentTimeMillis();

    QueryResponse response = moviesRepository.moviesQueryYG(year, genre);

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    Map<String, Object> result = new HashMap<>();
    result.put("Count", response.count());
    result.put("ScannedCount", response.scannedCount());
    result.put("TotalTimeSeconds", duration / 1000.0);

    return result;
  }

  @Post(value = "/createGSI", produces = MediaType.APPLICATION_JSON)
  public Map<String, Object> createGSI(
      @QueryValue String attributeName,
      @QueryValue String attributeType
  ) {
    long startTime = System.currentTimeMillis();

    UpdateTableResponse response = moviesRepository.createGSI(attributeName, attributeType);

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    Map<String, Object> result = new HashMap<>();
    result.put("TableUpdateResponse", response);
    result.put("TotalTimeSeconds", duration / 1000.0);

    return result;
  }

  @Get(value = "/queryByGenre", produces = MediaType.APPLICATION_JSON)
  public Map<String, Object> queryByGenre(String genre) {
    long startTime = System.currentTimeMillis();

    QueryResponse response = moviesRepository.queryByGenre(genre);

    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    Map<String, Object> result = new HashMap<>();
    result.put("QueryResponse.count", response.count());
    result.put("QueryResponse.scanned", response.scannedCount());
    result.put("QueryResponse", response);
    result.put("TotalTimeSeconds", duration / 1000.0);

    return result;
  }
}
