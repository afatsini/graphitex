defmodule Graphitex.Client do
  @moduledoc """
  A registered process that provides a network interface for graphite
  """

  require Logger

  @doc """
  Add a node and the service it provides to the directory.
  """
  @spec metric(number, [String.t]) :: nil
  def metric(value, namespace) when is_list(namespace) do
    metric({value, Enum.join(namespace, ".")})
  end

  @spec metric(number, binary) :: nil
  def metric(value, namespace) when is_binary(namespace) do
    metric({value, namespace})
  end

  @spec metric(number, binary|String.t, Float.t) :: nil
  def metric(value, namespace, ts) when is_float(ts) do
    metric({value, namespace, Float.round(ts, 1)})
  end

  @spec metric({number, binary|String.t, Float.t}) :: nil
  def metric(measurement), do: send(pack_msg(measurement))

  @spec metric_batch([{number, binary|String.t, Float.t}]) :: nil
  def metric_batch(batch) do
    bulk_mgs = batch
    |> Enum.map(&pack_msg/1)
    |> Enum.join("")
    send(bulk_mgs)
  end

  #
  # Private API
  #

  @spec pack_msg({number, String.t}) :: binary
  defp pack_msg({val, ns} = _arg) do
    pack_msg(val, ns, :os.system_time(:seconds))
  end

  @spec pack_msg({number, String.t, Float.t}) :: binary
  defp pack_msg({val, ns, ts} = _arg) do
    pack_msg(val, ns, ts)
  end

  @spec pack_msg(number, String.t, Float.t) :: binary
  defp pack_msg(val, ns, ts) do
    '#{ns} #{val} #{ts}\n'
  end

  def send(msg) do
    port = Application.get_env(:graphitex, :port, 2003)
    host = Application.get_env(:graphitex, :host)

    case :gen_udp.open(0, [:binary, active: false]) do
      {:ok, socket} ->
        :gen_udp.send(socket, host, port, msg)
        :gen_udp.close(socket)
      _ -> :ok
    end
  end
end
