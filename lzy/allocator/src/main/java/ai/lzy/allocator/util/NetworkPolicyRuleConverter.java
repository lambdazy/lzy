package ai.lzy.allocator.util;

import ai.lzy.allocator.alloc.impl.kuber.NetworkPolicyManager.PolicyRule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.TypeConverter;
import jakarta.inject.Singleton;

import java.util.Optional;
import java.util.Set;

@Singleton
public class NetworkPolicyRuleConverter implements TypeConverter<String, Set<PolicyRule>> {
    @Override
    public Optional<Set<PolicyRule>> convert(String object,
                                              Class<Set<PolicyRule>> targetType,
                                              ConversionContext context)
    {
        var mapper = new ObjectMapper();
        try {
            Set<PolicyRule> rule =
                mapper.readValue(object, mapper.getTypeFactory().constructCollectionType(Set.class, PolicyRule.class));
            return Optional.of(rule);
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }
}
