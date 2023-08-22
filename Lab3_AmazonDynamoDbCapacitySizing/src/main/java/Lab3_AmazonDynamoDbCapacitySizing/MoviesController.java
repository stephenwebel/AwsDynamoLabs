package Lab3_AmazonDynamoDbCapacitySizing;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import jakarta.inject.Inject;

@Controller("/movies")
public class MoviesController {

  private final MoviesRepository moviesRepository;

  private volatile boolean isQuerying = false;  // A flag to control the querying loop

  @Inject
  public MoviesController(MoviesRepository moviesRepository) {
    this.moviesRepository = moviesRepository;
  }

  @Get("/updateCapacity")
  public String updateTableCapacity() {
    try {
      moviesRepository.updateMoviesTableCapacity();
      String tableStatus = moviesRepository.getMoviesTableStatus();
      return "Table status: " + tableStatus;
    } catch (Exception e) {
      return "Failed to update table capacity: " + e.getMessage();
    }
  }

  @Get("/startQuery")
  public String startContinuousQuery() {
    if (isQuerying) {
      return "Querying is already running!";
    }

    isQuerying = true;

    // Start a new thread for continuous querying
    new Thread(() -> {
      while (isQuerying) {
        try {
          moviesRepository.queryItem();
        } catch (Exception e) {
          System.out.println("Erroring: " + e.toString());
        }
      }
    }).start();

    return "Continuous querying started!";
  }

  @Get("/stopQuery")
  public String stopContinuousQuery() {
    if (!isQuerying) {
      return "Querying is not currently running!";
    }

    isQuerying = false;
    return "Continuous querying stopped!";
  }

  @Get("/disableAutoScaling")
  public String disableAutoScaling() {
    try {
      moviesRepository.disableAutoScaling();
      return "Successfully disabled autoscaling for movies table";
    } catch (Exception e) {
      return "Failed to disable autoscaling: " + e.getMessage();
    }
  }

  @Get("/enableAutoScaling")
  public String enableAutoScaling() {
    try {
      moviesRepository.enableAutoScaling();
      return "Successfully enabled autoscaling for movies table";
    } catch (Exception e) {
      return "Failed to enable autoscaling: " + e.getMessage();
    }
  }
}
