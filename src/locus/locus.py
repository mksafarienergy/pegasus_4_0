from datetime import datetime
import time

from utils import *
from src.locus.locus_api import LocusApi
from src.logger.logger import *



class Locus(LocusApi):
    """Contains all functionality of the Locus ETL structure"""
    
    def __init__(self):
        LocusApi.__init__(self)

    
    def get_component_data(self, site_id: str) -> tuple[dict, set, set]:
        """
        Getting the component information of a site and keeping the 
            data that is needed. Also classifying the components by 
            their node type

        :param site_id: the monitoring id for the site
        :return component_details: { id: {name: str, nodeType: str, parentId: int (might be str)} }
        :return meter_component_ids: set of ids that are of type METER
        :return inverter_component_ids: set of ids that are of type INVERTER
        """

        components = LocusApi._get_site_components(self, site_id)
        component_details = {}
        meter_ids = set()
        inverter_ids = set()

        for component in components:
            component_id = component['id']

            component_details[component_id] = {}
            component_details[component_id]['name'] = str(component['name'])
            component_details[component_id]['nodeType'] = str(component['nodeType'])
            component_details[component_id]['parentId'] = str(component['parentId'])

            if component['nodeType'] == 'METER':
                meter_ids.add(component_id)
            elif component['nodeType'] == 'INVERTER':
                inverter_ids.add(component_id)

        return component_details, meter_ids, inverter_ids


    def get_meter_data(self, site_id: str, meter_component_ids: set, intervaled_timestamps: list[list[str]]) -> None:
        """
        """

        # for meter_component_id in meter_component_ids:
        #     available_component_data = self.
        for meter_id in meter_component_ids:
            available_meter_data = LocusApi()._get_data_available_for_component(meter_id)
            

    def testme(self):
        LocusApi._testme(self)
        print("Running Locus Test...")
        cd, md, ic = self.get_component_data('3678856')
        print(cd)
        print(md)
        print(ic)        

    def mock_main(self, proforma_df):
        """Used as a mock main, similar to testme but should be more concrete here"""

        start_time = time.time()

        set_up_logger() # "Wh_sum", 
        short_names = ["Wh_estDowntimeLoss_sum", "Wh_estClippingLoss_sum", "Wh_estSnowLoss_sum",
                   "Wh_estDegradLoss_sum", "Wh_estSoilingLoss_sum", "Wh_estShadingLoss_sum",
                   "Wh_estOtherLoss_sum", "Wh_estPartialDowntimeLoss_sum"]
        count = 0
        proforma_df = proforma_df[proforma_df['monitoring_platform'] == 'Locus'].astype({"site_id": int})
        all_basefields = []
        for site_id in proforma_df['site_id']:
            component_details, meter_ids, inverter_ids = self.get_component_data(site_id)
            for meter_id in meter_ids:
                basefields = LocusApi()._get_data_available_for_component(meter_id)
                for basefield in basefields:
                    # log(basefield)
                    for aggregation in basefield['aggregations']:
                        # log(aggregation)
                        # print(aggregations.get('shortName'))
                        # log(f'{str(type(aggregations.get("shortName")))} | {str(aggregations.get("shortName"))}')
                        # log(str(aggregations.get('shortName')))
                        if aggregation.get('shortName') in short_names:
                            log(f'{site_id} | {meter_id} | {aggregation["shortName"]}')
                all_basefields.extend(basefields)
            count += 1
            # if count > 10:
            #     break
        # log(str(all_basefields))
    
        run_time = time.time() - start_time
        log(str(run_time))
        print(run_time)
