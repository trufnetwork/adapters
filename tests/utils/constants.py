from pydantic import SecretStr

# Public-domain Anvil/Hardhat dev private key. Not a secret — used across
# test fakes so TNAccessBlock's signer construction (which sdk-py now
# validates strictly at init time) doesn't fail on unrelated code paths.
# Flagged here to silence secret scanners and avoid duplication.
FAKE_PRIVATE_KEY = SecretStr("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
