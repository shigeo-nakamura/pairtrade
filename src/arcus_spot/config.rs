use dex_connector::ArcusSpotPair;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ArcusSpotRuntimeMode {
    /// Evaluate public observations and emit plans without changing inventory.
    #[default]
    ReadOnly,
    /// Apply indicative fills to isolated in-memory inventory for deterministic replay.
    ReplaySimulation,
    /// Emit plans for an external, durable one-shot executor. Runtime state
    /// changes only after apply_confirmed_live_fill reconciles wallet balances.
    #[cfg(feature = "arcus-spot-live")]
    Live,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotInventory {
    /// Human token quantity for pair.sell_symbol.
    pub token_a: Decimal,
    /// Human token quantity for pair.buy_symbol.
    pub token_b: Decimal,
}

impl ArcusSpotInventory {
    pub fn checked_value_usd(
        self,
        token_a_price_usd: Decimal,
        token_b_price_usd: Decimal,
    ) -> Option<Decimal> {
        self.token_a
            .checked_mul(token_a_price_usd)?
            .checked_add(self.token_b.checked_mul(token_b_price_usd)?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArcusSpotRuntimeConfig {
    #[serde(default)]
    pub mode: ArcusSpotRuntimeMode,
    pub chain_id: u64,
    pub pair: ArcusSpotPair,
    pub notional_usd: Decimal,
    pub initial_inventory: ArcusSpotInventory,
    pub inventory_floors: ArcusSpotInventory,
    /// Maximum fraction of inventory above the applicable floor that one plan may sell.
    pub max_rotation_fraction: Decimal,
    pub signal_window_samples: usize,
    pub min_signal_samples: usize,
    pub entry_z_score: f64,
    pub exit_z_score: f64,
    pub max_quote_age_secs: i64,
    pub max_hold_secs: i64,
    /// Recorder route loss plus the two explicit buffers must not exceed this.
    pub max_all_in_round_trip_cost_bps: Decimal,
    pub gas_buffer_bps: Decimal,
    pub settlement_buffer_bps: Decimal,
    pub max_inventory_imbalance_fraction: Decimal,
    pub daily_loss_limit_usd: Decimal,
    pub cumulative_loss_limit_usd: Decimal,
}

impl ArcusSpotRuntimeConfig {
    pub fn normalize(&mut self) {
        self.pair.sell_symbol = self.pair.sell_symbol.trim().to_ascii_uppercase();
        self.pair.buy_symbol = self.pair.buy_symbol.trim().to_ascii_uppercase();
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.chain_id == 0 {
            return Err("chain_id must be non-zero".to_string());
        }
        if self.pair.sell_symbol.trim().is_empty()
            || self.pair.buy_symbol.trim().is_empty()
            || self
                .pair
                .sell_symbol
                .eq_ignore_ascii_case(&self.pair.buy_symbol)
        {
            return Err("pair must contain two distinct non-empty symbols".to_string());
        }
        if self.notional_usd <= Decimal::ZERO {
            return Err("notional_usd must be positive".to_string());
        }
        validate_inventory("initial_inventory", self.initial_inventory)?;
        validate_inventory("inventory_floors", self.inventory_floors)?;
        if self.inventory_floors.token_a > self.initial_inventory.token_a
            || self.inventory_floors.token_b > self.initial_inventory.token_b
        {
            return Err("inventory floors cannot exceed initial inventory".to_string());
        }
        if self.max_rotation_fraction <= Decimal::ZERO || self.max_rotation_fraction > Decimal::ONE
        {
            return Err("max_rotation_fraction must be in (0, 1]".to_string());
        }
        if self.signal_window_samples < 2 {
            return Err("signal_window_samples must be at least 2".to_string());
        }
        if self.min_signal_samples < 2 || self.min_signal_samples > self.signal_window_samples {
            return Err("min_signal_samples must be in [2, signal_window_samples]".to_string());
        }
        if !self.entry_z_score.is_finite() || self.entry_z_score <= 0.0 {
            return Err("entry_z_score must be finite and positive".to_string());
        }
        if !self.exit_z_score.is_finite()
            || self.exit_z_score < 0.0
            || self.exit_z_score >= self.entry_z_score
        {
            return Err("exit_z_score must be finite and in [0, entry_z_score)".to_string());
        }
        if self.max_quote_age_secs <= 0 || self.max_hold_secs <= 0 {
            return Err("quote age and hold limits must be positive".to_string());
        }
        for (name, value) in [
            (
                "max_all_in_round_trip_cost_bps",
                self.max_all_in_round_trip_cost_bps,
            ),
            ("gas_buffer_bps", self.gas_buffer_bps),
            ("settlement_buffer_bps", self.settlement_buffer_bps),
        ] {
            if value < Decimal::ZERO {
                return Err(format!("{name} cannot be negative"));
            }
        }
        if self.max_all_in_round_trip_cost_bps <= Decimal::ZERO {
            return Err("max_all_in_round_trip_cost_bps must be positive".to_string());
        }
        let fixed_buffers = self
            .gas_buffer_bps
            .checked_add(self.settlement_buffer_bps)
            .ok_or("cost buffers exceed Decimal range")?;
        if fixed_buffers > self.max_all_in_round_trip_cost_bps {
            return Err("gas + settlement buffers exceed the all-in cost limit".to_string());
        }
        if self.max_inventory_imbalance_fraction < Decimal::ZERO
            || self.max_inventory_imbalance_fraction > Decimal::ONE
        {
            return Err("max_inventory_imbalance_fraction must be in [0, 1]".to_string());
        }
        if self.daily_loss_limit_usd <= Decimal::ZERO
            || self.cumulative_loss_limit_usd <= Decimal::ZERO
        {
            return Err("daily and cumulative loss limits must be positive".to_string());
        }
        Ok(())
    }
}

fn validate_inventory(name: &str, inventory: ArcusSpotInventory) -> Result<(), String> {
    if inventory.token_a < Decimal::ZERO || inventory.token_b < Decimal::ZERO {
        return Err(format!("{name} quantities cannot be negative"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> ArcusSpotRuntimeConfig {
        ArcusSpotRuntimeConfig {
            mode: ArcusSpotRuntimeMode::ReadOnly,
            chain_id: 4663,
            pair: ArcusSpotPair {
                sell_symbol: "NVDA".to_string(),
                buy_symbol: "AMD".to_string(),
            },
            notional_usd: Decimal::from(5),
            initial_inventory: ArcusSpotInventory {
                token_a: Decimal::ONE,
                token_b: Decimal::ONE,
            },
            inventory_floors: ArcusSpotInventory {
                token_a: Decimal::new(1, 1),
                token_b: Decimal::new(1, 1),
            },
            max_rotation_fraction: Decimal::ONE,
            signal_window_samples: 20,
            min_signal_samples: 10,
            entry_z_score: 2.0,
            exit_z_score: 0.25,
            max_quote_age_secs: 30,
            max_hold_secs: 86_400,
            max_all_in_round_trip_cost_bps: Decimal::from(100),
            gas_buffer_bps: Decimal::from(5),
            settlement_buffer_bps: Decimal::from(5),
            max_inventory_imbalance_fraction: Decimal::new(8, 1),
            daily_loss_limit_usd: Decimal::from(2),
            cumulative_loss_limit_usd: Decimal::from(10),
        }
    }

    #[test]
    fn rejects_floor_above_inventory() {
        let mut config = valid_config();
        config.inventory_floors.token_a = Decimal::from(2);
        assert!(config.validate().unwrap_err().contains("floors"));
    }

    #[test]
    fn rejects_buffers_above_cost_limit() {
        let mut config = valid_config();
        config.gas_buffer_bps = Decimal::from(60);
        config.settlement_buffer_bps = Decimal::from(41);
        assert!(config.validate().unwrap_err().contains("buffers"));
    }
}
