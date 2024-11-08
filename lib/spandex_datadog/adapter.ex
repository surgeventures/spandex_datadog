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
  @spec distributed_context(conn :: Plug.Conn.t(), Tracer.opts()) ::
          {:ok, SpanContext.t()}
          | {:error, :no_distributed_trace}
  def distributed_context(%Plug.Conn{} = conn, _opts) do
    context = context_from_w3c_headers(conn) || context_from_datadog_headers(conn)

    case context do
      nil -> {:error, :no_distributed_trace}
      context -> {:ok, context}
    end
  end

  @impl Spandex.Adapter
  @spec distributed_context(headers :: Spandex.headers(), Tracer.opts()) ::
          {:ok, SpanContext.t()}
          | {:error, :no_distributed_trace}
  def distributed_context(headers, _opts) do
    context = context_from_w3c_headers(headers) || context_from_datadog_headers(headers)

    case context do
      nil -> {:error, :no_distributed_trace}
      context -> {:ok, context}
    end
  end

  defp context_from_datadog_headers(%Plug.Conn{} = conn) do
    trace_id = get_first_header(conn, "x-datadog-trace-id") |> parse_datadog_header()
    parent_id = get_first_header(conn, "x-datadog-parent-id") |> parse_datadog_header()
    priority = get_first_header(conn, "x-datadog-sampling-priority") |> parse_datadog_header() || @default_priority

    if trace_id && parent_id do
      %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}
    end
  end

  defp context_from_datadog_headers(headers) do
    trace_id = get_header(headers, "x-datadog-trace-id") |> parse_datadog_header()
    parent_id = get_header(headers, "x-datadog-parent-id") |> parse_datadog_header()
    priority = get_header(headers, "x-datadog-sampling-priority") |> parse_datadog_header() || @default_priority

    if trace_id && parent_id do
      %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}
    end
  end

  defp context_from_w3c_headers(%Plug.Conn{} = conn) do
    traceparent = get_first_header(conn, "traceparent")
    tracestate = get_first_header(conn, "tracestate")
    context_from_w3c_headers(traceparent, tracestate)
  end

  defp context_from_w3c_headers(headers) do
    traceparent = get_header(headers, "traceparent")
    tracestate = get_header(headers, "tracestate")
    context_from_w3c_headers(traceparent, tracestate)
  end

  defp context_from_w3c_headers(nil, _), do: nil

  defp context_from_w3c_headers(traceparent, tracestate) do
    [_version, trace_id, parent_id, _flags] = String.split(traceparent, "-")
    trace_id = decode_w3c_trace_id(trace_id)
    parent_id = decode_w3c_parent_id(parent_id)
    priority = w3c_priority(tracestate)
    %SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}
  rescue
    e ->
      Logger.error(
        "Failed to parse W3C headers, traceparent: #{inspect(traceparent)}, tracestate: #{inspect(tracestate)}, error: #{inspect(e)}"
      )

      nil
  end

  defp decode_w3c_trace_id(hex_string) do
    <<id::128>> = Base.decode16!(hex_string, case: :lower)
    id
  end

  defp decode_w3c_parent_id(hex_string) do
    <<id::64>> = Base.decode16!(hex_string, case: :lower)
    id
  end

  defp w3c_priority(nil = _tracestate), do: @default_priority

  defp w3c_priority(tracestate) do
    tracestate
    |> String.split(",")
    |> Enum.find_value(fn vendor_state ->
      case vendor_state do
        "dd=" <> value -> value
        _ -> nil
      end
    end)
    |> String.split(";")
    |> Enum.find_value(fn param ->
      case param do
        "s:" <> value -> value
        _ -> nil
      end
    end)
    |> String.to_integer()
  rescue
    e ->
      Logger.error("Failed to parse W3C priority, tracestate: #{inspect(tracestate)}, error: #{inspect(e)}")
      @default_priority
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

  @spec get_first_header(Plug.Conn.t(), String.t()) :: integer() | nil
  defp get_first_header(conn, header_name) do
    conn
    |> Plug.Conn.get_req_header(header_name)
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

  defp parse_datadog_header(header) when is_bitstring(header) do
    case Integer.parse(header) do
      {int, _} -> int
      _ -> nil
    end
  end

  defp parse_datadog_header(_header), do: nil

  defp tracing_headers(%SpanContext{trace_id: trace_id, parent_id: parent_id, priority: priority}) do
    [
      {"x-datadog-trace-id", to_string(trace_id)},
      {"x-datadog-parent-id", to_string(parent_id)},
      {"x-datadog-sampling-priority", to_string(priority)}
    ]
  end
end
