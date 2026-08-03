defmodule Parquex.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/mindreframer/parquex"
  @authors ["Roman Heinrich <roman.heinrich@gmail.com>"]

  def project do
    [
      app: :parquex,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      prune_code_paths: Mix.env() != :dev,
      start_permanent: Mix.env() == :prod,
      description:
        "Fast, bounded, and ergonomic Parquet streaming for Elixir on local and S3-compatible object storage, powered by Rust",
      source_url: @source_url,
      homepage_url: @source_url,
      authors: @authors,
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:rustler, "== 0.38.0", optional: true, runtime: false},
      {:rustler_precompiled, "== 0.8.4"},
      {:telemetry, "== 1.4.2"},
      {:ex_doc, "== 0.40.3", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        {"README.md", [filename: "readme", title: "Parquex"]},
        "CHANGELOG.md",
        "SECURITY.md",
        "docs/stores.md",
        "docs/parquet-files.md",
        "docs/datasets.md",
        "docs/telemetry.md",
        "docs/runtime.md",
        {"docs/architecture/README.md", [filename: "design", title: "Design"]},
        "docs/architecture/storage.md",
        "docs/architecture/streaming.md",
        "docs/architecture/native-runtime.md"
      ],
      source_ref: "v#{@version}",
      source_url: @source_url,
      authors: @authors,
      formatters: ["html"]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      maintainers: @authors,
      links: %{
        "Source" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      build_tools: ["mix", "cargo"],
      files:
        [
          "lib",
          "native/parquex_nif/src",
          "native/parquex_nif/Cargo.toml",
          "native/parquex_nif/Cargo.lock",
          ".cargo/config.toml",
          "rust-toolchain.toml",
          ".formatter.exs",
          "mix.exs",
          "README.md",
          "LICENSE",
          "CHANGELOG.md",
          "SECURITY.md",
          "docs"
        ] ++ Path.wildcard("checksum-Elixir.Parquex.Native.exs")
    ]
  end
end
