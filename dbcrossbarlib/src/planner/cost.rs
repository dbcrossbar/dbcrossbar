//! Representing conversion costs.

use std::{fmt, iter, ops::Add};

use ordered_float::NotNan;
#[cfg(test)]
use proptest::{arbitrary::any, num, strategy::Strategy};
#[cfg(test)]
use proptest_derive::Arbitrary;

/// The cost of a conversion.
///
/// This is guaranteed to implement `Ord`. If any calculations produce NaN or
/// similar values, it will panic.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(test, derive(Arbitrary))]
pub(crate) struct Cost(
    #[cfg_attr(
        test,
        proptest(
            // To generate a random cost, use non-negative f32 with no "weird"
            // values like Nan.
            strategy = "(num::f32::ZERO|num::f32::POSITIVE).prop_map(|f| NotNan::new(f).unwrap())"
        )
    )]
    NotNan<f32>,
);

impl Cost {
    /// Some things in life are free!
    ///
    /// This is the identity value for `Add` and `Sum`.
    pub(crate) fn free() -> Cost {
        Cost(NotNan::new(0.0).expect("0.0 should never be Nan"))
    }

    /// Calculate a penalized cost assuming that we'll need to have data present
    /// on the local machine.
    pub(crate) fn penalize_for_local_data(self) -> Cost {
        Cost(self.0 * 2.0)
    }

    /// Calculate a penalized cost assuming that all data will need to pass
    /// through a single stream/file at some point.
    pub(crate) fn penalize_for_parallelism_one(self) -> Cost {
        Cost(self.0 * 2.0)
    }

    /// Calculate a penalized cost for leaving a cloud (as far as we can tell).
    pub(crate) fn penalize_for_cloud_egress(self) -> Cost {
        Cost(self.0 * 2.0)
    }

    /// **Whole-path** penalty. When possible, reward loyalty to a single cloud
    /// vendor. This is a small penalty, but it's applied to entire conversion
    /// paths as a tie breaker.
    ///
    /// The goal here is to discourage using s3:// and gs:// in the same
    /// pipeline for no good reason at all. (And similar issues.)
    ///
    /// This would break Dijkstra's algorithm because it's a _non-local_ path
    /// cost. It would also complicate search pruning. It could probably be
    /// replaced by a purely local penalty if we knew what cloud `dbcrossbar`
    /// itself was running in.
    pub(crate) fn penalize_for_multiple_clouds(self) -> Cost {
        Cost(self.0 * 1.25)
    }
}

impl Add for Cost {
    type Output = Cost;

    fn add(self, rhs: Self) -> Self::Output {
        Cost(self.0 + rhs.0)
    }
}

impl Default for Cost {
    /// The default cost of a conversion.
    fn default() -> Cost {
        Cost(NotNan::new(1.0).expect("1.0 should never be NaN"))
    }
}

impl fmt::Debug for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Just use `Display`.
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl iter::Sum for Cost {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        iter.fold(Cost::free(), |total, cost| total + cost)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Even if we someday switch to a more complicated cost model, with separate
    // costs for CPU, networking, etc., we probably still want keep these
    // properties. If we decide to relax one of these properties, we'll want to
    // do so carefully and deliberately.
    proptest! {
        #[test]
        fn free_is_additive_identity(cost in any::<Cost>()) {
            assert_eq!(cost + Cost::free(), cost);
            assert_eq!(Cost::free() + cost, cost);
        }

        #[test]
        #[allow(clippy::eq_op)] // Clippy misses the point of this test.
        fn addition_is_commutative(c1 in any::<Cost>(), c2 in any::<Cost>()) {
            // "Do you know why? Because addition is commutative!"
            //
            // ObTomLehrer: https://www.youtube.com/watch?v=UIKGV2cTgqA
            assert_eq!(c1 + c2, c2 + c1);
        }

        #[test]
        fn addition_is_associative(
            c1 in any::<Cost>(),
            c2 in any::<Cost>(),
            c3 in any::<Cost>()
        ) {
            assert_eq!(c1 + (c2 + c3), (c1 + c2) + c3);
        }
    }

    #[test]
    fn sum_of_no_costs_is_free() {
        let no_costs: Vec<Cost> = vec![];
        assert_eq!(no_costs.into_iter().sum::<Cost>(), Cost::free());
    }
}
