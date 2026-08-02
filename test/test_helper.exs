if System.get_env("PARQUEX_RUSTFS_INTEGRATION") == "1" do
  ExUnit.start()
else
  ExUnit.start(exclude: [rustfs_integration: true])
end

Code.require_file("support/fixture_case.ex", __DIR__)
Code.require_file("support/resource_leak.ex", __DIR__)
Code.require_file("support/rustfs_case.ex", __DIR__)
