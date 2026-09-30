pub mod conversion;
pub mod product_revenue;
pub mod top_products;
pub mod user_activity;

// re-export so handlers can pull `params::TopProductQuery` without knowing about sub-files
pub use conversion::ConversionQuery;
pub use product_revenue::ProductRevenueQuery;
pub use top_products::{Source, TopProductQuery, mv_lower_bound};
pub use user_activity::UserActivityQuery;
