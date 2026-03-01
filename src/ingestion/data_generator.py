"""
Synthetic high-volume crypto data generator for stress testing.

Produces a fully-enriched Spark DataFrame via a cross join of coin metadata
and a dense hourly time series, with Spark-native random price/volume jitter.
This avoids CoinGecko API rate limits while generating 100K–1M+ rows.

Environment variables:
    STRESS_TEST_COINS  – number of unique coins to simulate (default: 200)
    STRESS_TEST_DAYS   – number of historical days (default: 30)
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType, DateType,
)

# ---------------------------------------------------------------------------
# Coin catalogue – 250 realistic tickers so callers can request up to 250
# ---------------------------------------------------------------------------
_COIN_CATALOGUE = [
    ("bitcoin",        "btc",  "Bitcoin",          67000.0,  1_300_000_000_000),
    ("ethereum",       "eth",  "Ethereum",          3500.0,    420_000_000_000),
    ("tether",         "usdt", "Tether",               1.0,    110_000_000_000),
    ("bnb",            "bnb",  "BNB",                600.0,     90_000_000_000),
    ("solana",         "sol",  "Solana",             170.0,     78_000_000_000),
    ("usd-coin",       "usdc", "USD Coin",             1.0,     43_000_000_000),
    ("xrp",            "xrp",  "XRP",                  0.5,     27_000_000_000),
    ("cardano",        "ada",  "Cardano",              0.45,    16_000_000_000),
    ("avalanche",      "avax", "Avalanche",           38.0,     16_000_000_000),
    ("dogecoin",       "doge", "Dogecoin",             0.15,    21_000_000_000),
    ("polkadot",       "dot",  "Polkadot",             8.0,     11_000_000_000),
    ("chainlink",      "link", "Chainlink",           15.0,      9_000_000_000),
    ("tron",           "trx",  "TRON",                 0.12,    10_500_000_000),
    ("shiba-inu",      "shib", "Shiba Inu",         0.000025,    15_000_000_000),
    ("litecoin",       "ltc",  "Litecoin",            90.0,      6_700_000_000),
    ("polygon",        "matic","Polygon",              0.85,      8_000_000_000),
    ("dai",            "dai",  "Dai",                  1.0,      5_500_000_000),
    ("bitcoin-cash",   "bch",  "Bitcoin Cash",       490.0,      9_600_000_000),
    ("cosmos",         "atom", "Cosmos",              11.0,      4_300_000_000),
    ("stellar",        "xlm",  "Stellar",              0.12,      3_400_000_000),
    ("near",           "near", "NEAR Protocol",        7.0,      7_700_000_000),
    ("monero",         "xmr",  "Monero",             180.0,      3_300_000_000),
    ("uniswap",        "uni",  "Uniswap",             11.0,      8_300_000_000),
    ("filecoin",       "fil",  "Filecoin",             6.5,      3_300_000_000),
    ("internet-computer","icp","Internet Computer",  12.0,      5_600_000_000),
    ("aptos",          "apt",  "Aptos",               11.0,      4_800_000_000),
    ("hedera",         "hbar", "Hedera",               0.12,      4_100_000_000),
    ("lido-dao",       "ldo",  "Lido DAO",             2.5,      2_200_000_000),
    ("optimism",       "op",   "Optimism",             2.8,      3_200_000_000),
    ("arbitrum",       "arb",  "Arbitrum",             1.3,      3_500_000_000),
    ("vechain",        "vet",  "VeChain",             0.038,     2_700_000_000),
    ("the-graph",      "grt",  "The Graph",            0.26,      2_500_000_000),
    ("algorand",       "algo", "Algorand",             0.19,      1_600_000_000),
    ("quant-network",  "qnt",  "Quant",              110.0,      1_650_000_000),
    ("flow",           "flow", "Flow",                 0.80,      1_200_000_000),
    ("theta-token",    "theta","Theta Network",        2.5,       2_500_000_000),
    ("sandbox",        "sand", "The Sandbox",          0.55,      1_300_000_000),
    ("decentraland",   "mana", "Decentraland",         0.55,      1_000_000_000),
    ("aave",           "aave", "Aave",               110.0,       1_600_000_000),
    ("maker",          "mkr",  "Maker",             3200.0,       3_100_000_000),
    ("compound",       "comp", "Compound",            75.0,         600_000_000),
    ("sushiswap",      "sushi","SushiSwap",            1.4,         350_000_000),
    ("curve-dao",      "crv",  "Curve DAO",            0.55,        700_000_000),
    ("1inch",          "1inch","1inch",                0.45,        430_000_000),
    ("render",         "rndr", "Render",               8.0,       3_500_000_000),
    ("injective",      "inj",  "Injective",           32.0,       3_100_000_000),
    ("sui",            "sui",  "Sui",                  1.85,      4_900_000_000),
    ("sei",            "sei",  "Sei",                  0.65,      1_600_000_000),
    ("starknet",       "strk", "Starknet",             1.0,       1_400_000_000),
    ("wormhole",       "w",    "Wormhole",             0.55,       750_000_000),
    # --- 50 more mid-cap coins ---
    ("harmony",        "one",  "Harmony",             0.015,      225_000_000),
    ("iota",           "miota","IOTA",                 0.22,       600_000_000),
    ("neo",            "neo",  "NEO",                 14.0,         990_000_000),
    ("chiliz",         "chz",  "Chiliz",               0.095,      530_000_000),
    ("enjin-coin",     "enj",  "Enjin Coin",           0.27,       460_000_000),
    ("zilliqa",        "zil",  "Zilliqa",              0.028,      430_000_000),
    ("basic-attention-token","bat","Basic Attention",  0.22,       360_000_000),
    ("storj",          "storj","Storj",                0.55,       220_000_000),
    ("ankr",           "ankr", "Ankr",                 0.045,      420_000_000),
    ("ocean-protocol", "ocean","Ocean Protocol",       0.85,       480_000_000),
    ("ren",            "ren",  "Ren",                  0.075,      150_000_000),
    ("band-protocol",  "band", "Band Protocol",        1.8,         180_000_000),
    ("numeraire",      "nmr",  "Numeraire",           23.0,         220_000_000),
    ("nucypher",       "nu",   "NuCypher",             0.085,        90_000_000),
    ("keep-network",   "keep", "Keep Network",         0.11,        110_000_000),
    ("fetch-ai",       "fet",  "Fetch.ai",             2.3,        2_000_000_000),
    ("singularitynet", "agix", "SingularityNET",       0.88,        550_000_000),
    ("ocean-v2",       "ocn",  "Ocean V2",             0.20,        120_000_000),
    ("civic",          "cvc",  "Civic",                0.16,        190_000_000),
    ("power-ledger",   "powr", "Power Ledger",         0.25,        230_000_000),
    ("loopring",       "lrc",  "Loopring",             0.22,        300_000_000),
    ("dydx",           "dydx", "dYdX",                 2.0,        600_000_000),
    ("blur",           "blur", "Blur",                 0.28,        450_000_000),
    ("gmx",            "gmx",  "GMX",                 35.0,         370_000_000),
    ("synthetix",      "snx",  "Synthetix",            2.5,         800_000_000),
    ("yearn-finance",  "yfi",  "Yearn Finance",      8500.0,        310_000_000),
    ("balancer",       "bal",  "Balancer",             3.5,         220_000_000),
    ("bancor",         "bnt",  "Bancor",               0.65,        120_000_000),
    ("kyber-network",  "knc",  "Kyber Network",        0.75,        120_000_000),
    ("zrx",            "zrx",  "0x Protocol",          0.42,        380_000_000),
    ("ethena",         "ena",  "Ethena",               0.98,       1_100_000_000),
    ("jito",           "jto",  "Jito",                 3.5,         480_000_000),
    ("pyth-network",   "pyth", "Pyth Network",         0.45,        600_000_000),
    ("jupiter",        "jup",  "Jupiter",              0.90,       1_200_000_000),
    ("raydium",        "ray",  "Raydium",              2.5,         700_000_000),
    ("bonk",           "bonk", "Bonk",               0.000025,      900_000_000),
    ("dogwifhat",      "wif",  "dogwifhat",            2.8,        2_800_000_000),
    ("pepe",           "pepe", "Pepe",             0.0000135,     5_700_000_000),
    ("floki",          "floki","FLOKI",             0.00022,         800_000_000),
    ("turbo",          "turbo","Turbo",             0.0055,          270_000_000),
    ("not",            "not",  "Notcoin",              0.016,       1_600_000_000),
    ("brett",          "brett","Brett",                0.17,         450_000_000),
    ("mog-coin",       "mog",  "Mog Coin",          0.0000022,       450_000_000),
    ("book-of-meme",   "bome", "Book of Meme",        0.012,         500_000_000),
    ("popcat",         "popcat","Popcat",               0.80,         800_000_000),
    ("cat-in-a-dogs-world","meow","cat in a dogs world",0.008,      200_000_000),
    ("gala",           "gala", "Gala",                 0.035,        530_000_000),
    ("illuvium",       "ilv",  "Illuvium",            80.0,          320_000_000),
    ("axie-infinity",  "axs",  "Axie Infinity",        7.5,          460_000_000),
    ("stepn",          "gmt",  "STEPN",                0.18,         390_000_000),
    ("gods-unchained", "gods", "Gods Unchained",       0.38,         180_000_000),
    ("immutable-x",    "imx",  "Immutable X",          1.8,        2_400_000_000),
    ("ronin",          "ron",  "Ronin",                2.9,          800_000_000),
    ("pixels",         "pixel","Pixels",               0.20,         200_000_000),
    # --- 50 more long-tail coins ---
    ("ontology",       "ont",  "Ontology",             0.25,        260_000_000),
    ("icon",           "icx",  "ICON",                 0.20,        250_000_000),
    ("kava",           "kava", "Kava",                 0.65,        530_000_000),
    ("terra-luna",     "luna", "Terra",                0.65,        680_000_000),
    ("oasis-network",  "rose", "Oasis Network",        0.10,        610_000_000),
    ("skale",          "skl",  "SKALE",                0.06,        300_000_000),
    ("celo",           "celo", "Celo",                 0.55,        290_000_000),
    ("nervos-network", "ckb",  "Nervos Network",       0.014,       330_000_000),
    ("kadena",         "kda",  "Kadena",               1.0,         190_000_000),
    ("horizen",        "zen",  "Horizen",              10.0,        130_000_000),
    ("secret",         "scrt", "Secret",               0.95,        150_000_000),
    ("deso",           "deso", "Decentralized Social", 14.0,         90_000_000),
    ("wax",            "waxp", "WAX",                  0.065,       140_000_000),
    ("ultraviolet",    "ultra","Ultra",                1.0,         190_000_000),
    ("mina-protocol",  "mina", "Mina Protocol",        0.80,        680_000_000),
    ("origintrail",    "trac", "OriginTrail",          0.70,        280_000_000),
    ("safemoon",       "sfm",  "SafeMoon",           0.00022,       130_000_000),
    ("hex",            "hex",  "HEX",                 0.004,        780_000_000),
    ("pulse-chain",    "pls",  "PulseChain",          0.00008,      200_000_000),
    ("hive",           "hive", "Hive",                 0.33,        120_000_000),
    ("steem",          "steem","Steem",                0.27,         95_000_000),
    ("dash",           "dash", "Dash",                30.0,         450_000_000),
    ("zcash",          "zec",  "Zcash",               27.0,         390_000_000),
    ("ravencoin",      "rvn",  "Ravencoin",            0.025,       195_000_000),
    ("digibyte",       "dgb",  "DigiByte",             0.009,       143_000_000),
    ("verge",          "xvg",  "Verge",               0.003,         45_000_000),
    ("siacoin",        "sc",   "Siacoin",             0.006,        115_000_000),
    ("xdc-network",    "xdc",  "XDC Network",          0.035,       420_000_000),
    ("tomochain",      "tomo", "TomoChain",            0.75,        110_000_000),
    ("nuls",           "nuls", "NULS",                 0.40,         70_000_000),
    ("wanchain",       "wan",  "Wanchain",             0.22,         70_000_000),
    ("ark",            "ark",  "Ark",                  0.55,         75_000_000),
    ("rise",           "rise", "Rise",                 0.05,         20_000_000),
    ("elastic",        "xel",  "Elastic",              0.10,         18_000_000),
    ("stratis",        "strax","Stratis",              0.18,         45_000_000),
    ("neblio",         "nebl", "Neblio",               0.22,         18_000_000),
    ("emercoin",       "emc",  "EmerCoin",             0.35,         18_000_000),
    ("pascal-coin",    "pasc", "Pascal Coin",          0.12,         14_000_000),
    ("megacoin",       "mega", "MegaCoin",             0.015,         8_000_000),
    ("primecoin",      "xpm",  "Primecoin",            0.12,         12_000_000),
    ("mooncoin",       "moon", "Mooncoin",           0.000005,       10_000_000),
    ("feathercoin",    "ftc",  "Feathercoin",          0.005,         8_000_000),
    ("namecoin",       "nmc",  "Namecoin",             0.60,         10_000_000),
    ("peercoin",       "ppc",  "Peercoin",             0.45,         10_000_000),
    ("novacoin",       "nvc",  "Novacoin",             0.35,          5_000_000),
    ("gridcoin",       "grc",  "Gridcoin",             0.004,         5_000_000),
    ("nxt",            "nxt",  "Nxt",                  0.01,          8_000_000),
    ("blackcoin",      "blk",  "BlackCoin",            0.008,         4_000_000),
    ("vericoin",       "vrc",  "VeriCoin",             0.12,          4_000_000),
    ("bitshares",      "bts",  "BitShares",            0.013,        38_000_000),
    ("burst",          "burst","Burstcoin",            0.005,         8_000_000),
    ("counterparty",   "xcp",  "Counterparty",        10.0,          15_000_000),
    ("nubits",         "usnbt","NuBits",               0.08,          4_000_000),
]


def generate_historical_data(
    spark: SparkSession,
    logger: logging.Logger,
    num_coins: Optional[int] = None,
    days: Optional[int] = None,
) -> DataFrame:
    """
    Generate a synthetic historical hourly crypto price DataFrame.

    Total rows = num_coins × (days × 24).
    Default: 200 coins × 30 days × 24h = 144,000 rows.
    Prices follow independent log-normal jitter (no serial dependency needed for
    a shuffle/IO stress test).

    Returns a DataFrame with the same schema as the enriched bronze layer:
        id, symbol, name, current_price, market_cap, total_volume,
        last_updated (string ISO-8601), _ingested_at (timestamp), partition_date (date)
    """
    num_coins = num_coins or int(os.environ.get("STRESS_TEST_COINS", 200))
    days = days or int(os.environ.get("STRESS_TEST_DAYS", 30))
    num_coins = min(num_coins, len(_COIN_CATALOGUE))
    hours_total = days * 24

    logger.info(
        "Generating synthetic data: %d coins × %d hours = %d rows",
        num_coins, hours_total, num_coins * hours_total,
    )

    # Use UTC so that partition_date and last_updated match the intended calendar days
    # when the DataFrame is written to Bronze (preserves 7 days → 7 partition folders).
    previous_tz = spark.conf.get("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        return _generate_historical_data_impl(
            spark, logger, num_coins, days, hours_total
        )
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous_tz)


def _generate_historical_data_impl(
    spark: SparkSession,
    logger: logging.Logger,
    num_coins: int,
    days: int,
    hours_total: int,
) -> DataFrame:
    """Internal implementation; session timeZone is assumed to be UTC."""
    # ---- Step 1: Coin metadata (tiny broadcast table) ---
    coin_schema = StructType([
        StructField("id",               StringType(), True),
        StructField("symbol",           StringType(), True),
        StructField("name",             StringType(), True),
        StructField("base_price",       DoubleType(), True),
        StructField("base_market_cap",  LongType(),   True),
    ])
    coins_df = spark.createDataFrame(_COIN_CATALOGUE[:num_coins], schema=coin_schema)

    # ---- Step 2: Hourly time index (dense range) ---
    # Epoch seconds for each hour in the range [now - days, now)
    now_epoch = int(
        datetime.now(tz=timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
        .timestamp()
    )
    start_epoch = now_epoch - days * 86_400

    time_df = spark.range(hours_total).select(
        (F.lit(start_epoch) + F.col("id") * 3600).alias("epoch_seconds"),
    ).select(
        F.from_unixtime(F.col("epoch_seconds")).alias("last_updated_ts"),
        F.to_date(F.from_unixtime(F.col("epoch_seconds"))).alias("partition_date"),
    )

    # ---- Step 3: Cross join – produces num_coins × hours_total rows ---
    cross_df = coins_df.crossJoin(F.broadcast(time_df) if hours_total <= 1440 else time_df)

    # ---- Step 4: Apply random jitter via Spark-native functions ---
    # Each row gets an independent log-normal multiplier for price and volume.
    # randn() seed differs per row due to Spark's per-partition random state.
    result_df = cross_df.select(
        F.col("id"),
        F.col("symbol"),
        F.col("name"),
        # price: base × e^(N(0, 0.08))  — ~8% std-dev per hourly tick
        (F.col("base_price") * F.exp(F.randn() * 0.08)).alias("current_price"),
        # market_cap: scale with price jitter
        (F.col("base_market_cap").cast(DoubleType()) * F.exp(F.randn() * 0.05))
            .cast(LongType()).alias("market_cap"),
        # volume: ~3-7% of market cap, log-normal spread
        (F.col("base_market_cap").cast(DoubleType()) * 0.05 * F.exp(F.randn() * 0.4))
            .cast(LongType()).alias("total_volume"),
        F.date_format(F.col("last_updated_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
            .alias("last_updated"),
        F.current_timestamp().alias("_ingested_at"),
        F.col("partition_date"),
    )

    logger.info(
        "Synthetic DataFrame plan ready (%d × %d = %d rows).",
        num_coins, hours_total, num_coins * hours_total,
    )
    return result_df
