defmodule SpandexDatadog.AgentHttpClient.Impl do
  def send_traces(%{host: host, port: port, body: body, headers: headers}) do
    Req.put("http://#{host}:#{port}/v0.4/traces",
      body: body,
      headers: headers,
      retry: false,
      connect_options: [
        timeout: 200
      ],
      pool_timeout: 200,
      receive_timeout: 200
    )
  end
end
