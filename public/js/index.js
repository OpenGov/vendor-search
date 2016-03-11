var max = 3500000;


var layer = L.tileLayer('http://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
  attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, &copy; <a href="http://cartodb.com/attributions">CartoDB</a>'
});

var map = L.map('map').setView([40.2313150803688, -82.7325439453125], 8);

var bounds = new L.Bounds();

test = _.sortBy(data, "total");
console.log(test);

map.addLayer(layer);

var circle = {
    radius: 8,
    fillColor: "#74acb8",
    color: "#555",
    weight: 1,
    opacity: 1,
    fillOpacity: 0.3
};


L.geoJson(data, {
   pointToLayer: function (feature, latlng) {
      console.log(latlng)
      bounds.extend( latlng )
      console.log(bounds)
      //console.log(feature.properties)

      var row = $("<tr />")
      $("#table").find("tbody").append(row)

      var total = feature.properties.total;

      if (total > max) {
        max = total;
      }

      var scale = d3.scale.linear()
         .domain([0, max])
         .range([100, 7000]);

      var color_scale = chroma.bezier(['D5F4BB', '#0C5314'])

      var scale2 = d3.scale.linear()
         .domain([0,max])
         .range([0, 1]);

       var scale3 = d3.scale.linear()
          .domain([0,max])
          .range([0.2, 0.5]);

      console.log("scale it: ", scale(total))

      var color = color_scale(scale2(total)).hex();

      circle.fillColor = color;
      circle.fillOpacity = scale3(total);
      circle.opacity = scale3(total) + 0.3;

      var area = scale(total);
      var radius = Math.sqrt(area / Math.PI)
      circle.radius = radius;

      _.each(feature.properties, function(prop, key){

         var cell = $("<td class=" + key + ">" + prop + "</td>")

         if (key == "total") {
            prop = "$" + prop;
         }

         if (key == "entity") {
            cell.prepend("<div class='color_key' style='background-color: " +  color + "'></div>")
         }


         row.append(cell);
      })

      return L.circleMarker(latlng, circle);
   }
}).addTo(map);
