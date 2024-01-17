defmodule OneBrc do
  def time() do
    :timer.tc(fn -> calculate_average() end)
    |> elem(0)
    |> Kernel./(1_000_000)
  end

  def calculate_average(path \\ "./measurements_50m.txt") do
    cores = :erlang.system_info(:logical_processors_available)
    divisor = cores + 1
    pattern = :binary.compile_pattern(<<";">>)

    tables =
      Enum.map(0..cores, fn core ->
        name = String.to_atom("brc_#{core}")
        table = :ets.new(name, [:set, :public])
        {core, table}
      end)
      |> Enum.into(%{})

    File.stream!(path)
    |> Stream.chunk_every(10_000)
    |> Stream.with_index()
    |> Task.async_stream(
      fn {list, index} ->
        key = rem(index, divisor)

        tables[key]
        |> insert_list(list, pattern)
      end,
      max_concurrency: cores
    )
    |> Stream.run()

    result =
      Enum.to_list(0..cores)
      |> List.foldl(%{}, fn key, acc ->
        table = tables[key]

        map =
          :ets.tab2list(table)
          |> Enum.into(%{})

        :ets.delete(table)

        if acc == %{} do
          map
        else
          Map.merge(acc, map, fn _, v1, v2 ->
            case v1 do
              nil ->
                v2

              [v1_min, v1_max, v1_sum, v1_count] ->
                [v2_min, v2_max, v2_sum, v2_count] = v2
                [min(v1_min, v2_min), max(v1_max, v2_max), v1_sum + v2_sum, v1_count + v2_count]
            end
          end)
        end
      end)
      |> Enum.map(fn {k, [min, max, sum, count]} ->
        mean = (sum / count) |> Float.round(1)
        {k, [min, mean, max]}
      end)
      |> Enum.sort_by(fn {k, _} -> k end, :asc)

    [
      "{",
      Enum.map(result, fn {k, [min, mean, max]} ->
        "#{k}=#{min}/#{mean}/#{max}"
      end)
      |> Enum.join(", "),
      "}"
    ]
    |> IO.puts()
  end

  def insert_list(table, list, pattern) do
    Stream.each(list, fn l ->
      [location, temperature] = split(l, pattern)

      :ets.lookup(table, location)
      |> case do
        [] ->
          # [min, max, sum, count]
          :ets.insert(table, {location, [temperature, temperature, temperature, 1]})

        [{_, [min, max, sum, count]}] ->
          :ets.insert(
            table,
            {location,
             [
               min(min, temperature),
               max(max, temperature),
               sum + temperature,
               count + 1
             ]}
          )
      end
    end)
    |> Stream.run()
  end

  def split(str, pattern \\ :binary.compile_pattern(<<";">>)) do
    str = String.trim_trailing(str)

    {i, _} = :binary.match(str, pattern)
    string = :binary.part(str, {0, i})

    <<_::binary-size(i + 1), rest::binary>> = str
    float = :erlang.binary_to_float(rest)

    [string, float]
  end
end
