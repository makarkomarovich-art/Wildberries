"""
Нормализация списка артикулов из Content API.

Функции:
- extract_triples_from_content_cards: для совместимости с Excel (четверки и пары)
- extract_data_for_supabase: для записи в Supabase (products + product_sizes)
"""

from typing import Any, Dict, Iterable, List, Tuple


def extract_triples_from_content_cards(cards: Iterable[Dict[str, Any]]) -> Tuple[List[Tuple[int, str, str, str]], List[Tuple[str, int]]]:
    """Извлекает уникальные четверки (nmID, barcode, vendorCode, size) и пары (vendorCode, nmID) из Content API.

    Важно: у одного (nmID, vendorCode) может быть несколько barcodes (разные размеры).
    Мы формируем по записи на каждый barcode из sizes[].skus (Array of strings).
    Пустые barcodes не добавляем.
    Размер берется из techSize или wbSize.
    """
    seen_quads = set()
    seen_pairs = set()
    out_quads: List[Tuple[int, str, str, str]] = []
    out_pairs: List[Tuple[str, int]] = []
    
    for card in cards:
        nm = card.get('nmID')
        if nm is None:
            continue
        nm = int(nm)
        vendor = str(card.get('vendorCode', '')).strip()
        
        # Добавляем пару (vendorCode, nmID) если она уникальна
        pair_key = (vendor, nm)
        if pair_key not in seen_pairs and vendor:  # vendor не должен быть пустым
            seen_pairs.add(pair_key)
            out_pairs.append(pair_key)
        
        sizes = card.get('sizes')
        if not isinstance(sizes, list):
            # Нет размеров/скю — пропускаем без добавления пустых баркодов
            continue
        for s in sizes:
            if not isinstance(s, dict):
                continue
                
            # Получаем размер из techSize или wbSize
            size = str(s.get('techSize', '')).strip()
            if not size:
                size = str(s.get('wbSize', '')).strip()
            if not size:
                size = 'Без размера'  # fallback для случаев без размера
                
            skus = s.get('skus')
            if not isinstance(skus, list):
                continue
            for sku in skus:
                if isinstance(sku, str):
                    barcode = sku.strip()
                elif isinstance(sku, dict):
                    barcode = str(sku.get('barcode', '')).strip()
                else:
                    continue
                if not barcode:
                    continue
                key = (nm, barcode, vendor, size)
                if key not in seen_quads:
                    seen_quads.add(key)
                    out_quads.append(key)
    return out_quads, out_pairs


def extract_data_for_supabase(cards: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Извлекает данные из Content API для записи в Supabase.
    
    Returns:
        Tuple[List[Dict], List[Dict]]: (products_data, product_sizes_data)
        
    products_data: список словарей с полями:
        - nm_id: int
        - imt_id: int
        - vendor_code: str
        - title: str
        - category_wb: str
        
    product_sizes_data: список словарей с полями:
        - nm_id: int (для связи с products)
        - barcode: str
        - size: str
    """
    products_data: List[Dict[str, Any]] = []
    product_sizes_data: List[Dict[str, Any]] = []
    seen_nm_ids = set()
    seen_barcodes = set()
    
    for card in cards:
        nm_id = card.get('nmID')
        if nm_id is None:
            continue
        nm_id = int(nm_id)
        
        # Данные для products (один раз на nmID)
        if nm_id not in seen_nm_ids:
            seen_nm_ids.add(nm_id)
            
            imt_id = card.get('imtID')
            if imt_id is None:
                print(f"⚠️ WARNING: Товар nmID={nm_id} не имеет imtID, пропускаем")
                continue
            
            vendor_code = str(card.get('vendorCode', '')).strip()
            if not vendor_code:
                print(f"⚠️ WARNING: Товар nmID={nm_id} не имеет vendorCode, пропускаем")
                continue
            
            title = str(card.get('title', '')).strip()
            if not title:
                print(f"⚠️ WARNING: Товар nmID={nm_id} не имеет title, пропускаем")
                continue
            
            category_wb = str(card.get('subjectName', '')).strip()
            if not category_wb:
                print(f"⚠️ WARNING: Товар nmID={nm_id} не имеет subjectName, пропускаем")
                continue
            
            products_data.append({
                'nm_id': nm_id,
                'imt_id': int(imt_id),
                'vendor_code': vendor_code,
                'title': title,
                'category_wb': category_wb
            })
        
        # Данные для product_sizes (все баркоды)
        sizes = card.get('sizes')
        if not isinstance(sizes, list):
            continue
        
        for size_item in sizes:
            if not isinstance(size_item, dict):
                continue
            
            # Размер (techSize) - может отсутствовать
            tech_size = size_item.get('techSize', '').strip() if size_item.get('techSize') else ''
            
            skus = size_item.get('skus')
            if not isinstance(skus, list):
                continue
            
            for sku in skus:
                if isinstance(sku, str):
                    barcode = sku.strip()
                elif isinstance(sku, dict):
                    barcode = str(sku.get('barcode', '')).strip()
                else:
                    continue
                
                if not barcode:
                    continue
                
                # Проверяем уникальность баркода
                if barcode in seen_barcodes:
                    continue
                seen_barcodes.add(barcode)
                
                product_sizes_data.append({
                    'nm_id': nm_id,  # для связи с products
                    'barcode': barcode,
                    'size': tech_size  # пустая строка если нет techSize
                })
    
    return products_data, product_sizes_data


