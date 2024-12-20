defmodule SpandexDatadog.Adapter do
  @moduledoc """
  A Datadog APM implementation for Spandex.
  """

  @behaviour Spandex.Adapter

  require Logger

  alias Spandex.{
    SpanContext,
    Tracer
  }

  @max_id 9_223_372_036_854_775_807
  @default_priority 1

  @impl Spandex.Adapter
  def trace_id(), do: :rand.uniform(@max_id)

  @impl Spandex.Adapter
  def span_id(), do: trace_id()

  @impl Spandex.Adapter
  def now(), do: :os.system_time(:nano_seconds)

  @impl Spandex.Adapter
  @spec default_sender() :: SpandexDatadog.ApiServer
  def default_sender() do
    SpandexDatadog.ApiServer
  end

  @doc """
  Fetches the Datadog-specific conn request headers if they are present.
  """
  @impl Spandex.Adapter
  @spec distributed_context(headers :: Plug.Conn.t() | Spandex.headers(), Tracer.opts()) ::
          {:ok, SpanContext.t()} | {:error, :no_distributed_trace}
  def distributed_context(headers, _opts) do
    context = context_from_w3c_headers(headers) || context_from_datadog_headers(headers)

    case context do
      nil -> {:error, :no_distributed_trace}
      context -> {:ok, context}
    end
  end

  defp context_from_datadog_headers(headers) do
    trace_id = get_header(headers, "x-datadog-trace-id") |> parse_integer()
    parent_id = get_header(headers, "x-datadog-parent-id") |> parse_integer()
    priority = get_header(headers, "x-datadog-sampling-priority") |> parse_integer() || @default_priority

    if trace_id && parent_id do
      %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}
    end
  end

  defp context_from_w3c_headers(headers) do
    traceparent = get_header(headers, "traceparent")
    tracestate = get_header(headers, "tracestate")
    context_from_w3c_headers(traceparent, tracestate)
  end

  defp context_from_w3c_headers(nil, _), do: nil

  defp context_from_w3c_headers(traceparent, tracestate) do
    [_version, trace_id, parent_id, _flags] = String.split(traceparent, "-")
    trace_id = decode_w3c_id(trace_id)
    parent_id = decode_w3c_id(parent_id)
    priority = w3c_priority(tracestate)
    %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}
  rescue
    e ->
      Logger.error(
        "Failed to parse W3C headers, traceparent: #{inspect(traceparent)}, tracestate: #{inspect(tracestate)}, error: #{inspect(e)}"
      )

      nil
  end

  defp decode_w3c_id(hex_string) do
    # Truncate 128-bit trace ID to last 64 bits as Datadog only uses lower 64 bits for trace correlation.
    # https://docs.datadoghq.com/opentelemetry/interoperability/otel_api_tracing_interoperability/#128-bit-trace-ids
    hex_string
    |> String.slice(-16, 16)
    |> String.to_integer(16)
  end

  defp w3c_priority(nil = _tracestate), do: @default_priority

  defp w3c_priority(tracestate) do
    with vendors <- String.split(tracestate, ~r/[ \t]*+,[ \t]*+/),
         {:ok, dd_tracestate} <-
           Enum.find_value(vendors, {:error, :dd_tracestate_not_found}, fn
             "dd=" <> value -> {:ok, value}
             _ -> nil
           end),
         dd_fields <- extract_datadog_fields(dd_tracestate) do
      Map.get(dd_fields, "s")
      |> case do
        nil ->
          @default_priority

        value ->
          parse_integer(value) ||
            (Logger.error("Failed to parse W3C priority, tracestate: #{inspect(tracestate)}}") && @default_priority)
      end
    else
      {:error, :dd_tracestate_not_found} -> @default_priority
    end
  end

  def extract_datadog_fields(dd_tracestate) do
    dd_tracestate
    |> String.split(";")
    |> Enum.reduce(%{}, fn pair, acc ->
      [key, value] = String.split(pair, ":", parts: 2)
      Map.put(acc, key, value)
    end)
  end

  @impl Spandex.Adapter
  @spec default_sampling_strategy() :: Spandex.SamplingStrategy.t()
  def default_sampling_strategy() do
    SpandexDatadog.SamplingStrategies.KeepAll
  end

  @doc """
  Injects Datadog-specific HTTP headers to represent the specified SpanContext
  """
  @impl Spandex.Adapter
  @spec inject_context([{term(), term()}], SpanContext.t(), Tracer.opts()) :: [{term(), term()}]
  def inject_context(headers, %SpanContext{} = span_context, _opts) when is_list(headers) do
    span_context
    |> tracing_headers()
    |> Kernel.++(headers)
  end

  def inject_context(headers, %SpanContext{} = span_context, _opts) when is_map(headers) do
    span_context
    |> tracing_headers()
    |> Enum.into(%{})
    |> Map.merge(headers)
  end

  # Private Helpers

  @spec get_header(Plug.Conn.t(), String.t()) :: integer() | nil
  defp get_header(%Plug.Conn{} = conn, key) do
    conn
    |> Plug.Conn.get_req_header(key)
    |> List.first()
  end

  @spec get_header(%{}, String.t()) :: integer() | nil
  defp get_header(headers, key) when is_map(headers) do
    Map.get(headers, key, nil)
  end

  @spec get_header([], String.t()) :: integer() | nil
  defp get_header(headers, key) when is_list(headers) do
    Enum.find_value(headers, fn {k, v} -> if k == key, do: v end)
  end

  defp parse_integer(value) when is_bitstring(value) do
    case Integer.parse(value) do
      {int, _} -> int
      _ -> nil
    end
  end

  defp parse_integer(_value), do: nil

  defp tracing_headers(%SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}) do
    [
      {"x-datadog-trace-id", to_string(trace_id)},
      {"x-datadog-parent-id", to_string(parent_id)},
      {"x-datadog-sampling-priority", to_string(priority)}
    ]
  end
end
