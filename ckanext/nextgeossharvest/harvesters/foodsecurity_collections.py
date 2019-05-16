COLLECTION_DESCRIPTIONS = {
    "NextGEOSS Sentinel-2 FAPAR":
    ("FAPAR corresponds to the fraction of photosynthetically active "
     "radiation absorbed by the canopy.The FAPAR value results directly "
     "from the radiative transfer model in the canopy which is computed "
     "instantaneously. It depends on canopy structure, vegetation element "
     "optical properties and illumination conditions. FAPAR is very useful "
     "as input to a number of primary productivity models which run at the daily "
     "time step. Consequently, the product definition should correspond to the "
     "daily integrated FAPAR value that can be approached by computation of the "
     "clear sky daily integrated FAPAR values as well as the FAPAR value computed "
     "for diffuse conditions. The SENTINEL 2 FAPAR product corresponds to the "
     "instantaneous black-sky around 10:15 which is a close approximation of the daily "
     "integrated black-sky FAPAR value. The FAPAR refers only to the green parts "
     "of the canopy."),
    "NextGEOSS Sentinel-2 FCOVER":
    ("Fraction of vegetation Cover (FCOVER) corresponds to the gap fraction "
     "for nadir direction. It is used to separate vegetation and soil in energy "
     "balance processes, including temperature and evapotranspiration. It is "
     "computed from the leaf area index and other canopy structural variables "
     "and does not depend on variables such as the geometry of illumination as "
     "compared to FAPAR. For this reason, it is a very good candidate for the replacement "
     "of classical vegetation indices for the monitoring of green vegetation. Because of "
     "the linear relationship with radiometric signal, FCOVER will be only marginally "
     "scale dependent. Note that similarly to LAI and FAPAR, only the green elements will "
     "be considered, either belonging both to the overstory and understory."),
    "NextGEOSS Sentinel-2 LAI":
    ("LAI was defined by CEOS as half the developed area of the convex "
     "hull wrapping the green canopy elements per unit horizontal ground. "
     "This definition allows accounting for elements which are not flat such "
     "as needles or stems. LAI is strongly non linearly related to reflectance. "
     "Therefore, its estimation from remote sensing observations will be scale "
     "dependent over heterogeneous landscapes. When observing a canopy made of "
     "different layers of vegetation, it is therefore mandatory to consider all "
     "the green layers. This is particularly important for forest canopies where "
     "the understory may represent a very significant contribution to the total "
     "canopy LAI. The derived LAI corresponds therefore to the total green LAI, "
     "including the contribution of the green elements of the understory. The "
     "resulting NEXTGEOSS SENTNEL LAI products are relatively consistent with the "
     "actual LAI for low LAI values and 'non-forest' surfaces; while for forests, "
     "particularly for needle leaf types, significant departures with the true LAI "
     "are expected."),
    "NextGEOSS Sentinel-2 NDVI":
    ("The SENTINEL-2 Normalized Difference Vegetation Index (NDVI) is a proxy "
     "to quantify the vegetation amount. It is defined as NDVI=(NIR-Red)/(NIR+Red) "
     "where NIR corresponds to the reflectance in the near infrared band , and Red "
     "to the reflectance in the red band. It is closely related to FAPAR and is "
     "little scale dependent."),
}


