{%- macro cents_to_dollars(column_parameter, round_parameter = 2) -%}
   round( 1.0 * {{column_parameter}} / 10, {{round_parameter}})
{%- endmacro -%}