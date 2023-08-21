package Lab2_AmazonDynamoDBScansQueriesandIndexes;

import com.amazonaws.regions.Region;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.ConfigurationProperties;
import javax.validation.constraints.NotBlank;

@Requires(property = "dynamodb.endpoint")
@ConfigurationProperties("dynamodb")
public interface DynamoConfiguration {
    @NotBlank
    String getEndpoint();

    default String getRegion(){
        return "US-EAST-1";
    }
}
