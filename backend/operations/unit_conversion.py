# Unit conversion utilities for weight and volume
from decimal import Decimal

# Weight conversion factors to convert to kilograms
WEIGHT_TO_KG = {
    'Kilogram': Decimal('1.0'),
    'Gram': Decimal('0.001'),
    'Pound': Decimal('0.45359237'),
    'Ounce': Decimal('0.02834952')
}

# Dimension conversion factors to convert to meters
DIMENSION_TO_METER = {
    'Meter': Decimal('1.0'),
    'Centimeter': Decimal('0.01'),
    'Millimeter': Decimal('0.001'),
    'Inch': Decimal('0.0254'),
    'Foot': Decimal('0.3048'),
    'Yard': Decimal('0.9144')
}

def convert_weight(weight, from_unit = "Kilogram", to_unit='Kilogram'):
    """
    Convert weight from one unit to another.
    
    Args:
        weight (Decimal or float): The weight value to convert
        from_unit (str): The unit to convert from ('Kilogram', 'Gram', 'Pound', 'Ounce')
        to_unit (str): The unit to convert to (default: 'Kilogram')
        
    Returns:
        Decimal: The converted weight value
    """
    if weight is None:
        return Decimal('0')
        
    # Convert to Decimal if not already
    weight = Decimal(str(weight)) if not isinstance(weight, Decimal) else weight
    
    # Convert to kg first (our base unit)
    kg_weight = weight * WEIGHT_TO_KG.get(from_unit, Decimal('1.0'))
    
    # Then convert from kg to target unit
    if to_unit == 'Kilogram':
        return round(kg_weight, 2)
    
    return round( (kg_weight / WEIGHT_TO_KG.get(to_unit, Decimal('1.0')) ) , 2)

def convert_dimension(dimension, from_unit = "Meter", to_unit='Meter'):
    """
    Convert a dimension from one unit to another.
    
    Args:
        dimension (Decimal or float): The dimension value to convert
        from_unit (str): The unit to convert from ('Meter', 'Centimeter', 'Millimeter', 'Inch', 'Foot', 'Yard')
        to_unit (str): The unit to convert to (default: 'Meter')
        
    Returns:
        Decimal: The converted dimension value
    """
    if dimension is None:
        return Decimal('0')
        
    # Convert to Decimal if not already
    dimension = Decimal(str(dimension)) if not isinstance(dimension, Decimal) else dimension
    
    # Convert to meters first (our base unit)
    m_dimension = dimension * DIMENSION_TO_METER.get(from_unit, Decimal('1.0'))
    
    
    # Then convert from meters to target unit
    if to_unit == 'Meter':
        return round(m_dimension, 2) 
    
    return round((m_dimension / DIMENSION_TO_METER.get(to_unit, Decimal('1.0'))) , 2)

def calculate_volume(length, width, height, unit):
    """
    Calculate volume from dimensions.
    
    Args:
        length (Decimal or float): Length dimension
        width (Decimal or float): Width dimension
        height (Decimal or float): Height dimension
        unit (str): The unit of the dimensions
        
    Returns:
        Decimal: Volume in cubic meters
    """
    # Convert all dimensions to meters
    length_m = convert_dimension(length, unit)
    width_m = convert_dimension(width, unit)
    height_m = convert_dimension(height, unit)
    
    # Calculate volume in cubic meters
    return length_m * width_m * height_m