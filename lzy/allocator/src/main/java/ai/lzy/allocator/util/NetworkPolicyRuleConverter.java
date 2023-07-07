package ai.lzy.allocator.util;

import ai.lzy.allocator.alloc.impl.kuber.NetworkPolicyManager.PolicyRule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.TypeConverter;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.Optional;

@Singleton
public class NetworkPolicyRuleConverter implements TypeConverter<String, List<PolicyRule>> {
    @Override
    public Optional<List<PolicyRule>> convert(String object,
                                              Class<List<PolicyRule>> targetType,
                                              ConversionContext context)
    {
        var mapper = new ObjectMapper();
        try {
            List<PolicyRule> rule =
                mapper.readValue(object, mapper.getTypeFactory().constructCollectionType(List.class, PolicyRule.class));
            return Optional.of(rule);
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }
}
