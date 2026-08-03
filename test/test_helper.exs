if System.get_env("PARQUEX_RUSTFS_INTEGRATION") == "1" do
  ExUnit.start()
else
  ExUnit.start(exclude: [rustfs_integration: true])
end
