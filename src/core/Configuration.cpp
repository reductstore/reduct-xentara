#include "reductstore_xentara/Configuration.hpp"

namespace reductstore_xentara
{

auto RetryPolicy::delayForAttempt(std::size_t attempt) const noexcept -> std::chrono::milliseconds
{
	if (initialBackoff.count() == 0 || initialBackoff >= maximumBackoff)
	{
		return initialBackoff;
	}
	auto delay = initialBackoff;
	for (std::size_t index = 0; index < attempt && delay < maximumBackoff; ++index)
	{
		if (delay.count() > maximumBackoff.count() / 2)
		{
			return maximumBackoff;
		}
		delay *= 2;
	}
	return delay;
}

auto validate(const ClientConfiguration& configuration) -> std::string
{
	if (!configuration.serverUrl.starts_with("http://") && !configuration.serverUrl.starts_with("https://"))
	{
		return "serverUrl must start with http:// or https://";
	}
	if (configuration.bucket.empty())
	{
		return "bucket must not be empty";
	}
	if (configuration.connectionTimeout.count() <= 0 || configuration.requestTimeout.count() <= 0 || configuration.shutdownTimeout.count() <= 0)
	{
		return "timeouts must be positive";
	}
	if (configuration.shutdownTimeout < configuration.requestTimeout)
	{
		return "shutdownTimeout must be at least requestTimeout";
	}
	if (configuration.maxBatchRecords == 0 || configuration.maxBatchPayloadBytes == 0 ||
		configuration.maxBatchMetadataBytes == 0 || configuration.maxBatchAge.count() <= 0)
	{
		return "batch limits must be positive";
	}
	if (configuration.maxQueueBytes == 0)
	{
		return "maxQueueBytes must be positive";
	}
	if (configuration.retry.maxAttempts == 0 || configuration.retry.initialBackoff.count() < 0 || configuration.retry.maximumBackoff < configuration.retry.initialBackoff)
	{
		return "retry policy is invalid";
	}
	return {};
}

auto entryName(std::string_view prefix, std::string_view entry) -> std::string
{
	if (prefix.empty())
	{
		return std::string(entry);
	}
	std::string result;
	result.reserve(prefix.size() + 1 + entry.size());
	result.append(prefix);
	result.push_back('/');
	result.append(entry);
	return result;
}

}
