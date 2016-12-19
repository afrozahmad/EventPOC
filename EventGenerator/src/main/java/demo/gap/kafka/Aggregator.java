package demo.gap.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import demo.gap.events.CartEvent;

public class Aggregator {
	private Map<String, Map<String, Integer>> map = new HashMap<>();

	// Sku_count
	public void add(CartEvent event) {

		String sku = event.getItem().getSku();
		if (map.containsKey(event.getCustomer().getCustomerId())) {
			Map<String, Integer> skuMap = map.get(event.getCustomer()
					.getCustomerId());

			if (skuMap.containsKey(sku)) {
				if (event.getEvent().equals("ADD")) {
					skuMap.put(sku, skuMap.get(sku)
							+ event.getItem().getQuantity());
				} else {
					skuMap.put(sku, skuMap.get(sku)
							- event.getItem().getQuantity());
				}
			} else {
				if (event.getEvent().equals("ADD")) {
					skuMap.put(sku,  event.getItem().getQuantity());
				} else {
					skuMap.put(sku, 0 - event.getItem().getQuantity());
				}
			}

		} else {

			HashMap<String, Integer> skuMap = new HashMap<>();
			if (event.getEvent().equals("ADD")) {
				skuMap.put(sku,  event.getItem().getQuantity());
			} else {
				skuMap.put(sku, 0 - event.getItem().getQuantity());
			}
			map.put(event.getCustomer().getCustomerId(), skuMap);
		}

	}

	public void printCart(File file) {
		map.forEach((id, skuMap) -> {
			skuMap.forEach((sku, count) -> {
				String out = "id: " + id + "Item : " + sku + " Count : "
						+ count;
				System.out.println(out);
				if (file != null) {

					try (BufferedWriter bw = new BufferedWriter(new FileWriter(
							file))) {

						bw.write(out);
						bw.newLine();

					} catch (IOException e) {

						e.printStackTrace();

					}

				}

			});
		});

	}

}
