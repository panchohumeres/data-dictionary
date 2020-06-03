=====================================
{{DBname}}
=====================================

{{DBdescription}}
{{index_collections_text}}

.. toctree::
   :maxdepth: 2
   :caption: Contents:

{% for name in indexes %}
      {{ name }}
      {{" "}}
{% endfor %}