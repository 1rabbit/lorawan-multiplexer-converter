use lrwn_filters::{DevAddrPrefix, EuiPrefix};

/// Allow/deny filters for DevAddr and JoinEUI.
///
/// Filter logic:
/// 1. If deny list matches, reject
/// 2. If allow list (prefixes) is non-empty and doesn't match, reject
/// 3. Otherwise, accept
#[derive(Default, Clone)]
pub struct AllowDenyFilters {
    pub dev_addr_prefixes: Vec<DevAddrPrefix>,
    pub dev_addr_deny: Vec<DevAddrPrefix>,
    pub join_eui_prefixes: Vec<EuiPrefix>,
    pub join_eui_deny: Vec<EuiPrefix>,
}

impl AllowDenyFilters {
    /// Returns true if the PHY payload should pass the filter.
    pub fn matches(&self, phy_payload: &[u8]) -> bool {
        // Parse the PHY payload to determine message type
        if phy_payload.is_empty() {
            return true;
        }

        let mhdr = phy_payload[0];
        let mtype = mhdr >> 5;

        match mtype {
            // JoinRequest (0x00)
            0b000 => self.matches_join_request(phy_payload),
            // UnconfirmedDataUp (0x02) or ConfirmedDataUp (0x04)
            0b010 | 0b100 => self.matches_data_uplink(phy_payload),
            // Other message types pass through
            _ => true,
        }
    }

    fn matches_join_request(&self, phy_payload: &[u8]) -> bool {
        // JoinRequest: MHDR(1) + JoinEUI(8) + DevEUI(8) + DevNonce(2) + MIC(4) = 23 bytes
        if phy_payload.len() < 23 {
            return true;
        }

        // JoinEUI is bytes 1-8 (little-endian in the payload)
        let mut join_eui: [u8; 8] = [0; 8];
        join_eui.copy_from_slice(&phy_payload[1..9]);

        // Check deny list first
        for prefix in &self.join_eui_deny {
            if prefix.is_match(join_eui) {
                return false;
            }
        }

        // Check allow list (if non-empty)
        if !self.join_eui_prefixes.is_empty() {
            for prefix in &self.join_eui_prefixes {
                if prefix.is_match(join_eui) {
                    return true;
                }
            }
            return false;
        }

        true
    }

    fn matches_data_uplink(&self, phy_payload: &[u8]) -> bool {
        // Data uplink: MHDR(1) + DevAddr(4) + FCtrl(1) + FCnt(2) + ... + MIC(4)
        if phy_payload.len() < 12 {
            return true;
        }

        // DevAddr is bytes 1-4 (little-endian in the payload)
        let mut dev_addr: [u8; 4] = [0; 4];
        dev_addr.copy_from_slice(&phy_payload[1..5]);

        // Check deny list first
        for prefix in &self.dev_addr_deny {
            if prefix.is_match(dev_addr) {
                return false;
            }
        }

        // Check allow list (if non-empty)
        if !self.dev_addr_prefixes.is_empty() {
            for prefix in &self.dev_addr_prefixes {
                if prefix.is_match(dev_addr) {
                    return true;
                }
            }
            return false;
        }

        true
    }
}

/// Allow/deny filters for Gateway ID.
#[derive(Default, Clone)]
pub struct GatewayIdFilters {
    pub allow: Vec<EuiPrefix>,
    pub deny: Vec<EuiPrefix>,
}

impl GatewayIdFilters {
    /// Returns true if the gateway ID should pass the filter.
    pub fn matches(&self, gateway_id_le: [u8; 8]) -> bool {
        // Check deny list first
        for prefix in &self.deny {
            if prefix.is_match(gateway_id_le) {
                return false;
            }
        }

        // Check allow list (if non-empty)
        if !self.allow.is_empty() {
            for prefix in &self.allow {
                if prefix.is_match(gateway_id_le) {
                    return true;
                }
            }
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_gateway_id_filters_empty() {
        let filters = GatewayIdFilters::default();
        assert!(filters.matches([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]));
    }

    #[test]
    fn test_gateway_id_filters_allow_only() {
        let filters = GatewayIdFilters {
            allow: vec![EuiPrefix::from_str("0102030405060708/64").unwrap()],
            deny: vec![],
        };
        // Exact match should pass
        assert!(filters.matches([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]));
        // Non-match should fail
        assert!(!filters.matches([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]));
    }

    #[test]
    fn test_gateway_id_filters_deny_only() {
        let filters = GatewayIdFilters {
            allow: vec![],
            deny: vec![EuiPrefix::from_str("0102030405060708/64").unwrap()],
        };
        // Denied gateway should fail
        assert!(!filters.matches([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]));
        // Other gateways should pass
        assert!(filters.matches([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]));
    }

    #[test]
    fn test_gateway_id_filters_deny_takes_precedence() {
        let filters = GatewayIdFilters {
            allow: vec![EuiPrefix::from_str("0102030400000000/32").unwrap()],
            deny: vec![EuiPrefix::from_str("0102030405060708/64").unwrap()],
        };
        // Allowed prefix but denied specific gateway should fail
        assert!(!filters.matches([0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]));
        // Allowed prefix, not denied should pass
        assert!(filters.matches([0x00, 0x00, 0x00, 0x00, 0x04, 0x03, 0x02, 0x01]));
    }

    #[test]
    fn test_allow_deny_filters_empty() {
        let filters = AllowDenyFilters::default();
        // Empty payload should pass
        assert!(filters.matches(&[]));
        // Any payload should pass with empty filters
        assert!(filters.matches(&[0x40, 0x01, 0x02, 0x03, 0x04, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]));
    }
}
