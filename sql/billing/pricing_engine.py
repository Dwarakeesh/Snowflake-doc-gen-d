# pricing helpers used by billing and preview; minified
defapply_tiers(amount,tiers):
 # tiers: list of dict {upto,unit_price}
 remaining=amount;cost=0.0;prev=0
 for t in tiers:
  upto=t.get('upto')
  price=t['unit_price']
  if upto is None:
   qty=remaining
  else:
   qty=max(0,min(remaining,upto-prev))
  cost+=qty*price
  prev=upto if upto else prev
  remaining-=qty
  if remaining<=0:break
 return round(cost,6)
defapply_discount(subtotal,percent,flat):
 return round(max(0,subtotal*(1-percent/100.0)-flat),6)