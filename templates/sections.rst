=====================================
{{DBname}}
=====================================

{{DBdescription}}
{{index_collections_text}}

External links 
-----------------------------

{% for link in DBlinks %}
* {{ link }}
{{" "}}
{% endfor %}

Index-Collections list
-----------------------------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

{% for name in indexes %}
      {{ name }}
      {{" "}}
{% endfor %}