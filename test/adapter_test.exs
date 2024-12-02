defmodule SpandexDatadog.Test.AdapterTest do
  use ExUnit.Case, async: true

  alias Spandex.SpanContext

  alias SpandexDatadog.{
    Adapter,
    Test.TracedModule,
    Test.Util
  }

  test "a complete trace sends spans" do
    TracedModule.trace_one_thing()

    spans = Util.sent_spans()

    Enum.each(spans, fn span ->
      assert span.service == :spandex_test
      assert span.meta.env == "test"
      assert span.meta.version == "v1"
    end)
  end

  test "a trace can specify additional attributes" do
    TracedModule.trace_with_special_name()

    assert(Util.find_span("special_name").service == :special_service)
  end

  test "a span can specify additional attributes" do
    TracedModule.trace_with_special_name()

    assert(Util.find_span("special_name_span").service == :special_span_service)
  end

  test "a complete trace sends a top level span" do
    TracedModule.trace_one_thing()
    span = Util.find_span("trace_one_thing/0")
    refute is_nil(span)
    assert span.service == :spandex_test
    assert span.meta.env == "test"
  end

  test "a complete trace sends the internal spans as well" do
    TracedModule.trace_one_thing()

    assert(Util.find_span("do_one_thing/0") != nil)
  end

  test "the parent_id for a child span is correct" do
    TracedModule.trace_one_thing()

    assert(Util.find_span("trace_one_thing/0").span_id == Util.find_span("do_one_thing/0").parent_id)
  end

  test "a span is correctly notated as an error if an exception occurs" do
    Util.can_fail(fn -> TracedModule.trace_one_error() end)

    assert(Util.find_span("trace_one_error/0").error == 1)
  end

  test "spans all the way up are correctly notated as an error" do
    Util.can_fail(fn -> TracedModule.error_two_deep() end)

    assert(Util.find_span("error_two_deep/0").error == 1)
    assert(Util.find_span("error_one_deep/0").error == 1)
  end

  test "successful sibling spans are not marked as failures when sibling fails" do
    Util.can_fail(fn -> TracedModule.two_fail_one_succeeds() end)

    assert(Util.find_span("error_one_deep/0", 0).error == 1)
    assert(Util.find_span("do_one_thing/0").error == 0)
    assert(Util.find_span("error_one_deep/0", 1).error == 1)
  end

  describe "distributed_context/2 with Plug.Conn and DD headers" do
    test "returns a SpanContext struct" do
      conn =
        :get
        |> Plug.Test.conn("/")
        |> Plug.Conn.put_req_header("x-datadog-trace-id", "123")
        |> Plug.Conn.put_req_header("x-datadog-parent-id", "456")
        |> Plug.Conn.put_req_header("x-datadog-sampling-priority", "2")

      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(conn, [])
      assert span_context.trace_id == 123
      assert span_context.parent_id == 456
      assert span_context.priority == 2
    end

    test "priority defaults to 1 (i.e. we currently assume all distributed traces should be kept)" do
      conn =
        :get
        |> Plug.Test.conn("/")
        |> Plug.Conn.put_req_header("x-datadog-trace-id", "123")
        |> Plug.Conn.put_req_header("x-datadog-parent-id", "456")

      assert {:ok, %SpanContext{priority: 1}} = Adapter.distributed_context(conn, [])
    end
  end

  describe "distributed_context/2 with Plug.Conn and W3C headers" do
    test "returns a SpanContext struct" do
      conn =
        :get
        |> Plug.Test.conn("/")
        |> Plug.Conn.put_req_header("traceparent", "00-672ce69300000000d2af5a72993ea4b4-b7ad6b7169203331-00")
        |> Plug.Conn.put_req_header("tracestate", "dd=s:2;t.dm:-0;p:26251d1e51321aa0")

      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(conn, [])
      assert span_context.trace_id == 15_181_452_317_133_022_388
      assert span_context.parent_id == 13_235_353_014_750_950_193
      assert span_context.priority == 2
      assert encode_w3c_id(span_context.trace_id) == "d2af5a72993ea4b4"
      assert encode_w3c_id(span_context.parent_id) == "b7ad6b7169203331"
    end

    test "priority defaults to 1 when no tracestate" do
      conn =
        :get
        |> Plug.Test.conn("/")
        |> Plug.Conn.put_req_header("traceparent", "00-0000000000000000160bc62487e24d01-3702f1bcf6862126-01")

      assert {:ok, %SpanContext{priority: 1}} = Adapter.distributed_context(conn, [])
      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(conn, [])
      assert span_context.trace_id == 1_588_581_153_779_109_121
      assert span_context.parent_id == 3_963_996_415_931_588_902
      assert span_context.priority == 1
      assert encode_w3c_id(span_context.trace_id) == "160bc62487e24d01"
      assert encode_w3c_id(span_context.parent_id) == "3702f1bcf6862126"
    end

    test "priority defaults to 1 when no priority param in tracestate" do
      conn =
        :get
        |> Plug.Test.conn("/")
        |> Plug.Conn.put_req_header("traceparent", "00-0000000000000000160bc62487e24d01-3702f1bcf6862126-01")
        |> Plug.Conn.put_req_header("tracestate", "dd=t.dm:-0;p:121bcd413432")

      assert {:ok, %SpanContext{priority: 1}} = Adapter.distributed_context(conn, [])
      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(conn, [])
      assert span_context.trace_id == 1_588_581_153_779_109_121
      assert span_context.parent_id == 3_963_996_415_931_588_902
      assert span_context.priority == 1
      assert encode_w3c_id(span_context.trace_id) == "160bc62487e24d01"
      assert encode_w3c_id(span_context.parent_id) == "3702f1bcf6862126"
    end

    test "returns an error when it cannot parse traceparent" do
      conn =
        :get
        |> Plug.Test.conn("/")
        |> Plug.Conn.put_req_header("traceparent", "incorrectformat")
        |> Plug.Conn.put_req_header("tracestate", "incorrectformat")

      assert {:error, :no_distributed_trace} = Adapter.distributed_context(conn, [])
    end
  end

  describe "distributed_context/2 with Spandex.headers() and DD headers" do
    test "returns a SpanContext struct when headers is a list" do
      headers = [{"x-datadog-trace-id", "123"}, {"x-datadog-parent-id", "456"}, {"x-datadog-sampling-priority", "2"}]

      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(headers, [])
      assert span_context.trace_id == 123
      assert span_context.parent_id == 456
      assert span_context.priority == 2
    end

    test "returns a SpanContext struct when headers is a map" do
      headers = %{
        "x-datadog-trace-id" => "123",
        "x-datadog-parent-id" => "456",
        "x-datadog-sampling-priority" => "2"
      }

      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(headers, [])
      assert span_context.trace_id == 123
      assert span_context.parent_id == 456
      assert span_context.priority == 2
    end

    # for traces that are not explicitly started but rather are continued from a distributed context
    # we rather default to a priority of 1. because the real reason for the lack of sampling is in the upstream
    # and a default like that is both safer and makes it noticeable that the configuration might be wrong
    test "priority defaults to 1" do
      headers = %{
        "x-datadog-trace-id" => "123",
        "x-datadog-parent-id" => "456"
      }

      assert {:ok, %SpanContext{priority: 1}} = Adapter.distributed_context(headers, [])
    end
  end

  describe "distributed_context/2 with Spandex.headers() and W3C headers" do
    test "returns a SpanContext struct when headers is a list" do
      headers = [
        {"traceparent", "00-672ce69300000000d2af5a72993ea4b4-b7ad6b7169203331-00"},
        {"tracestate", "dd=s:2;t.dm:-0;p:26251d1e51321aa0"}
      ]

      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(headers, [])
      assert span_context.trace_id == 15_181_452_317_133_022_388
      assert span_context.parent_id == 13_235_353_014_750_950_193
      assert span_context.priority == 2
      assert encode_w3c_id(span_context.trace_id) == "d2af5a72993ea4b4"
      assert encode_w3c_id(span_context.parent_id) == "b7ad6b7169203331"
    end

    test "returns a SpanContext struct when headers is a map" do
      headers = %{
        "traceparent" => "00-672ce69300000000d2af5a72993ea4b4-b7ad6b7169203331-00",
        "tracestate" => "dd=s:2;t.dm:-0;p:26251d1e51321aa0"
      }

      assert {:ok, %SpanContext{} = span_context} = Adapter.distributed_context(headers, [])
      assert span_context.trace_id == 15_181_452_317_133_022_388
      assert span_context.parent_id == 13_235_353_014_750_950_193
      assert span_context.priority == 2
      assert encode_w3c_id(span_context.trace_id) == "d2af5a72993ea4b4"
      assert encode_w3c_id(span_context.parent_id) == "b7ad6b7169203331"
    end

    # for traces that are not explicitly started but rather are continued from a distributed context
    # we rather default to a priority of 1. because the real reason for the lack of sampling is in the upstream
    # and a default like that is both safer and makes it noticeable that the configuration might be wrong
    test "priority defaults to 1" do
      headers = %{
        "traceparent" => "00-672ce69300000000d2af5a72993ea4b4-b7ad6b7169203331-00"
      }

      assert {:ok, %SpanContext{priority: 1}} = Adapter.distributed_context(headers, [])
    end
  end

  describe "distributed_context/2 without headers present" do
    test "returns an error when Plug.Conn headers are empty" do
      conn = Plug.Test.conn(:get, "/")
      assert {:error, :no_distributed_trace} = Adapter.distributed_context(conn, [])
    end

    test "returns an error when Spandex.headers() are empty" do
      headers = %{}
      assert {:error, :no_distributed_trace} = Adapter.distributed_context(headers, [])
    end
  end

  describe "inject_context/3" do
    test "Prepends distributed tracing headers to an existing list of headers" do
      span_context = %SpanContext{trace_id: 123, parent_id: 456, priority: 10}
      headers = [{"header1", "value1"}, {"header2", "value2"}]

      result = Adapter.inject_context(headers, span_context, [])

      assert result == [
               {"x-datadog-trace-id", "123"},
               {"x-datadog-parent-id", "456"},
               {"x-datadog-sampling-priority", "10"},
               {"header1", "value1"},
               {"header2", "value2"}
             ]
    end

    test "Merges distributed tracing headers with an existing map of headers" do
      span_context = %SpanContext{trace_id: 123, parent_id: 456, priority: 10}
      headers = %{"header1" => "value1", "header2" => "value2"}

      result = Adapter.inject_context(headers, span_context, [])

      assert result == %{
               "x-datadog-trace-id" => "123",
               "x-datadog-parent-id" => "456",
               "x-datadog-sampling-priority" => "10",
               "header1" => "value1",
               "header2" => "value2"
             }
    end
  end

  defp encode_w3c_id(id) do
    Base.encode16(<<id::64>>, case: :lower)
  end
end
