select
    id as payment_id,
    "orderID" as order_id,
    "paymentMethod" as payment_method,
    amount/100::DECIMAL as amount_usd

from raw.stripe.payment