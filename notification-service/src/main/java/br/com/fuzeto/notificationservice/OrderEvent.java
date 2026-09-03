package br.com.fuzeto.notificationservice;

public record OrderEvent(
   Long orderId,
   String customer,
   String product,
   Double price
) {}
