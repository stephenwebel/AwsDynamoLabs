package Lab3_AmazonDynamoDbCapacitySizing;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import javax.validation.constraints.NotBlank;

@Requires(property = "dynamodb.endpoint")
@ConfigurationProperties("dynamodb")
public interface DynamoConfiguration {

  @NotBlank
  String getEndpoint();

  default String getRegion() {
    return "US-EAST-1";
  }
}
