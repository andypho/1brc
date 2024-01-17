# 1Brc in Elixir

To Run this: 

```
iex -S mix
iex(1)> OneBrc.calculate_average(path)
```

Using `String.split/2` was quite slow, using `benchee` I found out using erlang `binary.match/2` was the fastest.

# Environment / Spec
- Elixir 1.15.6
- Erlang/OTP 26
- Ubuntu 22.04.3 LTS running on WSL
- AMD Ryzen 7 3700X

## My Result
10m file: 4.3 (sec)
50m file: 22.7 (sec)
1b file: 389 (sec)

## TODO
- CreateMeasurements (currently generated using the python program provided in https://github.com/gunnarmorling/1brc)
