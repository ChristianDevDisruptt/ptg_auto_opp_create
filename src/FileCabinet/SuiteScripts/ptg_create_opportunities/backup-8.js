/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(['N/format', 'N/record', 'N/search'],
    /**
   * @param{format} format
   * @param{record} record
   * @param{search} search
   */
    (format, record, search) => {
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            try {
                //Obtener los clientes que tienen aviso o programado para generar su oportunidad
                //test prueba nueva laptop
                let customerSearchObj = search.create({
                    type: "customer",
                    filters:
                        [
                            // ["stage", "anyof", "CUSTOMER"],
                            // "AND",
                            // ["custentityptg_tipodecontacto_", "anyof", "2", "4"]
                            ["stage", "anyof", "CUSTOMER"],
                            "AND",
                            ["shippingaddress.custrecord_ptg_tipo_contacto", "anyof", "2", "4"],
                            "AND",
                            ["internalid", "anyof", "15838"]
                        ],
                    columns:
                        [
                            search.createColumn({ name: "internalid", label: "Internal ID" }),
                            //Primera versión
                            // search.createColumn({ name: "custentity_ptg_periododecontacto_", label: "PTG - Periodo de contacto:" }),
                            // search.createColumn({ name: "custentity_ptg_entrelas_", label: "PTG - Entre las:" }),
                            // search.createColumn({ name: "custentity_ptg_ylas_", label: "PTG - Y las_" }),
                            // search.createColumn({ name: "custentityptg_tipodecontacto_", label: "PTG - Tipo de Contacto:" }),
                            // search.createColumn({ name: "custentity_ptg_lunes_", label: "PTG - Lunes" }),
                            // search.createColumn({ name: "custentity_ptg_martes_", label: "PTG - Martes" }),
                            // search.createColumn({ name: "custentity_ptg_miercoles_", label: "PTG - Miercoles" }),
                            // search.createColumn({ name: "custentity_ptg_jueves_", label: "PTG - Jueves" }),
                            // search.createColumn({ name: "custentity_ptg_viernes_", label: "PTG - Viernes" }),
                            // search.createColumn({ name: "custentity_ptg_sabado_", label: "PTG - Sabado" }),
                            // search.createColumn({ name: "custentity_ptg_domingo_", label: "PTG - Domingo" }),
                            // search.createColumn({ name: "custentity_ptg_tipodeservicio_", label: "PTG - Tipo de Servicio" }),
                            // search.createColumn({ name: "subsidiary", label: "Primary Subsidiary" }),
                            // search.createColumn({ name: "custentity_ptg_articulo_frecuente", label: "PTG - ARTÍCULO FRECUENTE" }),
                            // search.createColumn({ name: "custentity_ptg_cantidad_frecuente_lt_cil", label: "PTG - CANTIDAD FRECUENTE LT / CIL" }),
                            // search.createColumn({ name: "custentity_ptg_tipodecliente_", label: "PTG - Tipo de cliente" }),
                            // search.createColumn({ name: "custentity_ptg_alianza_comercial_cliente", label: "PTG - ALIANZA COMERCIAL DEL CLIENTE" }),
                            // search.createColumn({ name: "address", label: "Address" }),
                            //Primera versión

                            //Segunda versión
                            // search.createColumn({
                            //     name: "custrecord_ptg_colonia_ruta",
                            //     join: "Address",
                            //     label: "PTG - COLONIA Y RUTA"
                            // }),
                            // // search.createColumn({ name: "datecreated", label: "Date Created" }),
                            // // search.createColumn({ name: "custentity_ptg_cada_", label: "PTG - Cada:" })
                            // search.createColumn({
                            //     name: "custrecord_ptg_lunes",
                            //     join: "Address",
                            //     label: "PTG - LUNES"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_martes",
                            //     join: "Address",
                            //     label: "PTG - MARTES"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_miercoles",
                            //     join: "Address",
                            //     label: "PTG - MIERCOLES"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_jueves",
                            //     join: "Address",
                            //     label: "PTG - JUEVES"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_viernes",
                            //     join: "Address",
                            //     label: "PTG - VIERNES"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_sabado",
                            //     join: "Address",
                            //     label: "PTG - SABADO"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_domingo",
                            //     join: "Address",
                            //     label: "PTG - DOMINGO"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_cada",
                            //     join: "Address",
                            //     label: "PTG - CADA"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_periodo_contacto",
                            //     join: "Address",
                            //     label: "PTG - PERIODO DE CONTACTO"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_entre_las",
                            //     join: "Address",
                            //     label: "PTG - ENTRE LAS"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_y_las",
                            //     join: "Address",
                            //     label: "PTG - Y LAS"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_tipo_contacto",
                            //     join: "Address",
                            //     label: "PTG - TIPO DE CONTACTO"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_tipo_servicio",
                            //     join: "Address",
                            //     label: "PTG - TIPO DE SERVICIO"
                            //  }),
                            //  search.createColumn({
                            //     name: "custrecord_ptg_capacidad_art",
                            //     join: "Address",
                            //     label: "PTG - CAPACIDAD DE ARTICULO"
                            //  }),
                            //  search.createColumn({name: "subsidiary", label: "Primary Subsidiary"}),
                            //  search.createColumn({name: "datecreated", label: "Date Created"}),
                            //  search.createColumn({name: "custentity_ptg_articulo_frecuente", label: "PTG - ARTÍCULO FRECUENTE"}),
                            //  search.createColumn({name: "custentity_ptg_tipodecliente_", label: "PTG - Tipo de cliente"}),
                            //  search.createColumn({name: "custentity_ptg_alianza_comercial_cliente", label: "PTG - ALIANZA COMERCIAL DEL CLIENTE"})
                            //Segunda versión
                            //Tercera Versión
                            search.createColumn({
                                name: "custrecord_ptg_colonia_ruta",
                                join: "Address",
                                label: "PTG - COLONIA Y RUTA"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_lunes",
                                join: "Address",
                                label: "PTG - LUNES"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_martes",
                                join: "Address",
                                label: "PTG - MARTES"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_miercoles",
                                join: "Address",
                                label: "PTG - MIERCOLES"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_jueves",
                                join: "Address",
                                label: "PTG - JUEVES"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_viernes",
                                join: "Address",
                                label: "PTG - VIERNES"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_sabado",
                                join: "Address",
                                label: "PTG - SABADO"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_domingo",
                                join: "Address",
                                label: "PTG - DOMINGO"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_cada",
                                join: "Address",
                                label: "PTG - CADA"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_periodo_contacto",
                                join: "Address",
                                label: "PTG - PERIODO DE CONTACTO"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_entre_las",
                                join: "Address",
                                label: "PTG - ENTRE LAS"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_y_las",
                                join: "Address",
                                label: "PTG - Y LAS"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_tipo_servicio",
                                join: "Address",
                                label: "PTG - TIPO DE SERVICIO"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_articulo_frecuente",
                                join: "Address",
                                label: "PTG - ARTICULO FRECUENTE"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_capacidad_art",
                                join: "Address",
                                label: "PTG - CAPACIDAD O CANTIDAD DEL ARTICULO"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_articulo_frecuente2",
                                join: "Address",
                                label: "PTG - ARTICULO FRECUENTE 2"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_capacidad_can_articulo_2",
                                join: "Address",
                                label: "PTG - CAPACIDAD O CANTIDAD DEL ARTICULO 2"
                            }),
                            search.createColumn({ name: "subsidiary", label: "Primary Subsidiary" }),
                            search.createColumn({ name: "datecreated", label: "Date Created" }),
                            search.createColumn({ name: "custentity_ptg_tipodecliente_", label: "PTG - Tipo de cliente" }),
                            search.createColumn({ name: "custentity_ptg_alianza_comercial_cliente", label: "PTG - ALIANZA COMERCIAL DEL CLIENTE" }),
                            search.createColumn({
                                name: "custrecord_ptg_tipo_contacto",
                                join: "Address",
                                label: "PTG - TIPO DE CONTACTO"
                            }),
                            search.createColumn({
                                name: "addressinternalid",
                                join: "Address",
                                label: "Address Internal ID"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_en_la_semana",
                                join: "Address",
                                label: "PTG - EN LA SEMANA"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio_mar",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO MAR"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio_mi",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO MI"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio_jue",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO JUE"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio_vi",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO VI"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio_sab",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO SAB"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_fecha_inicio_servicio_dom",
                                join: "Address",
                                label: "PTG -FECHA DE INICIO DE SERVICIO DOM"
                            }),
                            search.createColumn({
                                name: "internalid",
                                join: "Address",
                                label: "Internal ID"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_ruta_asignada",
                                join: "Address",
                                label: "PTG - RUTA ASIGNADA"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_ruta_asignada2",
                                join: "Address",
                                label: "PTG - RUTA ASIGNADA 2"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_ruta_asignada_3",
                                join: "Address",
                                label: "PTG - RUTA ASIGNADA 3"
                            }),
                            search.createColumn({
                                name: "custrecord_ptg_ruta_asignada_4",
                                join: "Address",
                                label: "PTG - RUTA ASIGNADA 4"
                            }),
                            search.createColumn({ name: "custentity_ptg_plantarelacionada_", label: "PTG - Planta relacionada: " })
                        ]
                });

                let searchResultCount = customerSearchObj.runPaged().count;
                if (searchResultCount > 0) {
                    return customerSearchObj
                }
                //log.debug("customerSearchObj result count", searchResultCount);
                // customerSearchObj.run().each(function (result) {
                //     // .run().each has a limit of 4,000 results
                //     log.debug('result', result)
                //     return true;
                // });    
            } catch (error) {
                log.debug('err', error)
            }


        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            try {
                //log.debug('mapContext', mapContext)
                let infoCustomer = JSON.parse(mapContext.value)
                log.debug('infoCustomer', infoCustomer)

                let week = []
                let sunday = (infoCustomer.values["custrecord_ptg_domingo.Address"] == 'T') ? week.push(0) : null
                let monday = (infoCustomer.values["custrecord_ptg_lunes.Address"] == 'T') ? week.push(1) : null
                let tuesday = (infoCustomer.values["custrecord_ptg_martes.Address"] == 'T') ? week.push(2) : null
                let wednesday = (infoCustomer.values["custrecord_ptg_miercoles.Address"] == 'T') ? week.push(3) : null
                let thursday = (infoCustomer.values["custrecord_ptg_jueves.Address"] == 'T') ? week.push(4) : null
                let friday = (infoCustomer.values["custrecord_ptg_viernes.Address"] == 'T') ? week.push(5) : null
                let saturday = (infoCustomer.values["custrecord_ptg_sabado.Address"] == 'T') ? week.push(6) : null

                let dates = []
                let sundayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_dom.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_dom.Address"]) : dates.push(0)
                let mondayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio.Address"]) : dates.push(0)
                let tuesdayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_mar.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_mar.Address"]) : dates.push(0)
                let wednesdayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_mi.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_mi.Address"]) : dates.push(0)
                let thursdayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_jue.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_jue.Address"]) : dates.push(0)
                let fridayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_vi.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_vi.Address"]) : dates.push(0)
                let saturdayDay = (!!infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_sab.Address"]) ? dates.push(infoCustomer.values["custrecord_ptg_fecha_inicio_servicio_sab.Address"]) : dates.push(0)

                let inThatWeek = infoCustomer.values["custrecord_ptg_en_la_semana.Address"];
                //week.push(monday, tuesday, wednesday, thursday, friday, saturday, sunday);
                log.debug('week of program services', week)
                log.debug('dates of program services', dates)
                log.debug('en la semana...', inThatWeek)
                //log.debug('week of program services', week.length)

                //Validamos si es de tipo programado o aviso
                let contactType = infoCustomer.values["custrecord_ptg_tipo_contacto.Address"].value;
                let clientType = (!!infoCustomer.values.custentity_ptg_tipodecliente_) ? infoCustomer.values.custentity_ptg_tipodecliente_.value : -1;
                let clienAlliance = (!!infoCustomer.values.custentity_ptg_alianza_comercial_cliente) ? infoCustomer.values.custentity_ptg_alianza_comercial_cliente.value : -1;
                infoCustomer.values.inThatWeek = inThatWeek
                infoCustomer.values.week = week
                infoCustomer.values.dates = dates
                //log.debug('contactType', contactType)

                //Validamos que sea el tipo programado o aviso
                if (Number(contactType) == 4 && (clienAlliance == 1 || clienAlliance == 2 || clientType == 3)) {
                    log.debug('progromado', infoCustomer)
                    //Validar si es día, semana o mensual
                    let typeService = infoCustomer.values["custrecord_ptg_tipo_servicio.Address"];
                    let typeFrequency = infoCustomer.values["custrecord_ptg_periodo_contacto.Address"].value;
                    let customer = infoCustomer.values.internalid.value;
                    let existOP = validExistOp(Number(customer), Number(contactType), infoCustomer.values["addressinternalid.Address"]);
                    log.debug('existOP antes del switch', existOP)
                    switch (typeFrequency) {
                        //Dias
                        case "1":
                            log.debug('dia', typeFrequency)
                            //Validar si le toca hoy el servicio 
                            // let makeServiceDay = validServiceAll(typeFrequency, week, infoCustomer.values.datecreated);
                            // log.debug('makeServiceDay', makeServiceDay)
                            //nueva validacion si es la fecha en que le toca servicio
                            let makeServiceDay = validServiceAll(infoCustomer, typeFrequency, week, dates);
                            log.debug('makeserviceweek', makeServiceWeek)
                            //Validamos el tipo de servicio si es cilindro , estacionario o mixto
                            log.debug('existOP día programado', existOP)
                            if (!!typeService && Number(typeService.value) == 1 && makeServiceDay && !existOP) {
                                log.debug('typeService', 'clindro')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 2 && makeServiceDay && !existOP) {
                                log.debug('typeService', 'estacionario')
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 4 && makeServiceDay && !existOP) {
                                log.debug('typeService', 'ambos')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            }

                            break;
                        //Semanal
                        case "2":
                            log.debug('semana', typeFrequency)
                            //Validar si le toca hoy el servicio                             
                            //let makeServiceWeek = validService(typeFrequency, week, infoCustomer.values.datecreated, Number(infoCustomer.values["custrecord_ptg_cada.Address"]));

                            //nueva validacion si es la fecha en que le toca servicio
                            let makeServiceWeek = validServiceAll(infoCustomer, typeFrequency, week, dates);
                            log.debug('makeserviceweek', makeServiceWeek)
                            //Validamos el tipo de servicio si es cilindro , estacionario o mixto y que no tenga creada una oportunidad                                                                                    
                            //log.debug('existOP', existOP)
                            if (!!typeService && Number(typeService.value) == 1 && makeServiceWeek && !existOP) {
                                log.debug('typeService', 'clindro')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 2 && makeServiceWeek && !existOP) {
                                log.debug('typeService', 'estacionario')
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 4 && makeServiceWeek && !existOP) {
                                log.debug('typeService', 'ambos')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            }

                            break;
                        //Mensual
                        case "3":
                            log.debug('mensual', typeFrequency)
                            //Validar si le toca hoy el servicio 
                            // let makeServiceMounth = validService(typeFrequency, week, infoCustomer.values.datecreated);
                            // log.debug('makeServiceMounth', makeServiceMounth)
                            //nueva validacion si es la fecha en que le toca servicio
                            let makeServiceMounth = validServiceAll(infoCustomer, typeFrequency, week, dates);
                            log.debug('makeServiceMounth', makeServiceMounth)

                            //Validamos el tipo de servicio si es cilindro , estacionario o mixto                         
                            if (!!typeService && Number(typeService.value) == 1 && makeServiceMounth && !existOP) {
                                log.debug('typeService', 'clindro')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 2 && makeServiceMounth && !existOP) {
                                log.debug('typeService', 'estacionario')
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 4 && makeServiceMounth && !existOP) {
                                log.debug('typeService', 'ambos')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            }

                            break;
                    }

                } else if (Number(contactType) == 2) {
                    log.debug('aviso', infoCustomer)
                    let typeService = infoCustomer.values["custrecord_ptg_tipo_servicio.Address"];
                    let typeFrequency = infoCustomer.values["custrecord_ptg_periodo_contacto.Address"].value;
                    let customer = infoCustomer.values.internalid.value;
                    let existOP = validExistOp(Number(customer), Number(contactType), infoCustomer.values["addressinternalid.Address"]);
                    switch (typeFrequency) {
                        //Dias
                        case "1":
                            log.debug('dia', typeFrequency)
                            //Validar si le toca hoy el servicio 
                            // let makeServiceDay = validService(typeFrequency, week, infoCustomer.values.datecreated);
                            // log.debug('makeServiceDay', makeServiceDay)
                            // log.debug('existOP día aviso', existOP)
                            //nueva validacion si es la fecha en que le toca servicio
                            let makeServiceDay = validServiceAll(infoCustomer, typeFrequency, week, dates);
                            log.debug('makeServiceDay', makeServiceDay)
                            //Validamos el tipo de servicio si es cilindro , estacionario o mixto
                            if (!!typeService && Number(typeService.value) == 1 && makeServiceDay && !existOP) {
                                log.debug('typeService', 'clindro')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 2 && makeServiceDay && !existOP) {
                                log.debug('typeService', 'estacionario')
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 4 && makeServiceDay && !existOP) {
                                log.debug('typeService', 'ambos')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            }

                            break;
                        //Semanal
                        case "2":
                            log.debug('semana', typeFrequency)
                            //Validar si le toca hoy el servicio                             
                            // let makeServiceWeek = validServiceAviso(typeFrequency, week, infoCustomer.values.datecreated, Number(infoCustomer.values["custrecord_ptg_cada.Address"]));
                            // log.debug('makeservice', makeServiceWeek)
                            //nueva validacion si es la fecha en que le toca servicio
                            let makeServiceWeek = validServiceAll(infoCustomer, typeFrequency, week, dates);
                            log.debug('makeServiceWeek', makeServiceWeek)
                            //Validamos el tipo de servicio si es cilindro , estacionario o mixto y que no tenga creada una oportunidad                                                                                    
                            log.debug('existOP', existOP)
                            if (!!typeService && Number(typeService.value) == 1 && makeServiceWeek && !existOP) {
                                log.debug('typeService', 'clindro')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 2 && makeServiceWeek && !existOP) {
                                log.debug('typeService', 'estacionario')
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 4 && makeServiceWeek && !existOP) {
                                log.debug('typeService', 'ambos')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            }

                            break;
                        //Mensual
                        case "3":
                            log.debug('mensual', typeFrequency)
                            //Validar si le toca hoy el servicio 
                            // let makeServiceMounth = validService(typeFrequency, week, infoCustomer.values.datecreated);
                            // log.debug('makeServiceMounth', makeServiceMounth)
                            let makeServiceMounth = validServiceAll(infoCustomer, typeFrequency, week, dates);
                            log.debug('makeServiceMounth', makeServiceMounth)
                            //Validamos el tipo de servicio si es cilindro , estacionario o mixto                         
                            if (!!typeService && Number(typeService.value) == 1 && makeServiceMounth && !existOP) {
                                log.debug('typeService', 'clindro')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 2 && makeServiceMounth && !existOP) {
                                log.debug('typeService', 'estacionario')
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            } else if (!!typeService && Number(typeService.value) == 4 && makeServiceMounth && !existOP) {
                                log.debug('typeService', 'ambos')
                                makeOPCilindro(typeService.value, infoCustomer, Number(contactType));
                                makeOPEstacionario(typeService.value, infoCustomer, Number(contactType));
                            }

                            break;
                    }
                }

            } catch (error) {
                log.debug('err map', error)
            }

        }

        //Validar el tipo de servicio y frecuencia
        const validService = (type, weeks, date, frequency) => {
            log.debug('validService type', type)
            log.debug('frequency type', frequency)
            let day = new Date();

            switch (type) {
                //Dias
                case "1":
                    let days = (day.getDay() == 0) ? 7 : day.getDay();
                    if (weeks.includes(days)) {
                        return true;
                    }
                    break;
                //Semanal
                case "2":
                    let week = (day.getDay() == 0) ? 7 : day.getDay();
                    let initWeek = getWeek(date);
                    let today = getWeek(day);
                    log.debug('initWeek', initWeek);
                    log.debug('today', today);
                    let resultMultiple = isMultiple(initWeek, today, frequency);
                    log.debug('resultMultiple', resultMultiple)
                    if (weeks.includes(week) && resultMultiple) {
                        return true;
                    }
                    break;
                //Mensual
                case "3":
                    let initialFormattedDateString = date;
                    //log.debug('initialFormattedDateString', initialFormattedDateString)
                    let parsedDateStringAsRawDateObject = format.parse({
                        value: initialFormattedDateString,
                        type: format.Type.DATE,
                        timezone: format.Timezone.AMERICA_MEXICO_CITY
                    });

                    let initDate = parsedDateStringAsRawDateObject.getDate();
                    let todayMounth = day.getDate();
                    log.debug('initDate mounth', initDate)
                    log.debug('todayMounth mounth', todayMounth)
                    if (initDate == todayMounth) {
                        return true;
                    }
                    break;
            }

        }

        //Este tiene la especial de ser para aviso semanal, ya que si la semana no se encuentra hoy pero mañana si, se tiene que crear
        const validServiceAviso = (type, weeks, date, frequency) => {
            let day = new Date();

            switch (type) {
                //Dias
                case "1":
                    let days = (day.getDay() == 0) ? 7 : day.getDay();
                    if (weeks.includes(days)) {
                        return true;
                    }
                    break;
                //Semanal
                case "2":
                    let week = (day.getDay() == 0) ? 7 : day.getDay();
                    let nextWeek = (week == 7) ? 1 : week + 1;
                    let initWeek = getWeek(date);
                    let today = getWeek(day);
                    log.debug('initWeek', initWeek);
                    log.debug('today', today);
                    let resultMultiple = isMultiple(initWeek, today, frequency);
                    log.debug('resultMultiple', resultMultiple)
                    if ((weeks.includes(week) || weeks.includes(nextWeek)) && resultMultiple) {
                        return true;
                    }
                    break;
                //Mensual
                case "3":
                    let initialFormattedDateString = date;
                    //log.debug('initialFormattedDateString', initialFormattedDateString)
                    let parsedDateStringAsRawDateObject = format.parse({
                        value: initialFormattedDateString,
                        type: format.Type.DATE,
                        timezone: format.Timezone.AMERICA_MEXICO_CITY
                    });

                    let initDate = parsedDateStringAsRawDateObject.getDate();
                    let todayMounth = day.getDate();
                    log.debug('initDate mounth', initDate)
                    log.debug('todayMounth mounth', todayMounth)
                    if (initDate == todayMounth) {
                        return true;
                    }
                    break;
            }

        }


        //Nueva validacion para el caso de diario, semanal y mensual        
        const validServiceAll = (customer, type, weeks, dates) => {
            let date = new Date();
            log.debug('data customer', customer);
            log.debug('date actual', date)
            let nowDate = format.parse({
                value: date,
                type: format.Type.DATETIME,
                //timezone: format.Timezone.AMERICA_MEXICO_CITY
            });

            let day = nowDate.getDay();
            log.debug('nowDate', nowDate);
            log.debug('day', day)
            const arrayIds = ['custrecord_ptg_fecha_inicio_servicio_dom',
                'custrecord_ptg_fecha_inicio_servicio',
                'custrecord_ptg_fecha_inicio_servicio_mar',
                'custrecord_ptg_fecha_inicio_servicio_mi',
                'custrecord_ptg_fecha_inicio_servicio_jue',
                'custrecord_ptg_fecha_inicio_servicio_vi',
                'custrecord_ptg_fecha_inicio_servicio_sab'
            ];
            switch (type) {
                //Dias
                case "1":
                    if (weeks.includes(day) || weeks.includes(day + 1)) {
                        let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                        log.debug('initialhour turn', initialHour);
                        let hour = getTimeInt(initialHour);
                        log.debug('hour', hour);
                        (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;
                        return true;
                    }
                    break;
                //Semanal
                case "2":
                    // log.debug('data customer', customer);
                    // log.debug('date actual', date)
                    // let nowDate = format.parse({
                    //     value: date,
                    //     type: format.Type.DATETIME,
                    //     timezone: format.Timezone.AMERICA_MEXICO_CITY
                    // });

                    // let day = nowDate.getDay();
                    // log.debug('nowDate', nowDate);
                    // log.debug('day', day)
                    // let formatCustomerDate = format.parse({
                    //     value: "29/4/2022",
                    //     type: format.Type.DATE,
                    //     timezone: format.Timezone.AMERICA_MEXICO_CITY
                    // })

                    // log.debug('formatCustomerDate', formatCustomerDate);
                    // log.debug('date customer', formatCustomerDate.getDay());

                    //Primero validamos que estemos en la semana en la que el cliente tenga un servicio ya sea hoy o mañana
                    //let weeks = JSON.parse(customer.week);
                    if (weeks.includes(day)) {
                        //Obtenemos la fecha del ultimo servicio por el numero de semana
                        log.debug('estamos en la semana', day);
                        log.debug('ultima fecha del servicio', dates[day]);

                        let converLastDate = format.parse({
                            value: dates[day],
                            type: format.Type.DATE,
                            timezone: format.Timezone.AMERICA_MEXICO_CITY
                        });

                        //Validamos cuantas veces por semana tenemos que hacer el servicio
                        let frecuencyWeek = Number(customer.values['custrecord_ptg_cada.Address']) * 7;
                        log.debug('frecuencyWeek', frecuencyWeek)
                        let lastDate = converLastDate.getDate();
                        log.debug('lastDate', lastDate);
                        //let compareDate = nowDate.setDate(nowDate.getDate() - frecuencyWeek).getDate()
                        //log.debug('plus frecuency week at date now', new Date(nowDate.setDate(nowDate.getDate() - frecuencyWeek)));
                        let lessDate = new Date(nowDate.setDate(nowDate.getDate() - frecuencyWeek));
                        log.debug('lessDate', lessDate);
                        log.debug('lessDate date', lessDate.getDate());
                        //Validamos que la diferencia de la fecha de hoy, sea igual a la fecha del ultimo servicio
                        if (lessDate.getDate() == lastDate) {
                            //Tienen la misma fecha y ahora toca definir la hora para saber que turno sera
                            let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                            log.debug('initialhour turn', initialHour);
                            let hour = getTimeInt(initialHour);
                            log.debug('hour', hour);
                            (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;

                            let values = {};
                            let fieldId = arrayIds[day]
                            values[fieldId] = nowDate;

                            log.debug('values update', values);
                            log.debug('customer edited', customer)

                            // record.submitFields({
                            //     type: 'address',
                            //     id: customer.values['internalid.Address'].value,
                            //     values: values
                            // });
                            try {
                                //Cargar el cliente para actualizar la fecha
                                let customerRecord = record.load({
                                    type: record.Type.CUSTOMER,
                                    id: customer.values.internalid.value,
                                    isDynamic: true
                                });

                                let lineAddress = customerRecord.findSublistLineWithValue({
                                    sublistId: 'addressbook',
                                    fieldId: 'addressbookaddress',
                                    value: customer.values["internalid.Address"].value
                                });

                                log.debug('id lineAddress', lineAddress)
                                customerRecord.selectLine({
                                    sublistId: 'addressbook',
                                    line: lineAddress
                                });

                                let addressSubrecord = customerRecord.getCurrentSublistSubrecord({
                                    sublistId: 'addressbook',
                                    fieldId: 'addressbookaddress'
                                });

                                log.debug('test fecha actual', addressSubrecord.getValue({
                                    fieldId: arrayIds[day]
                                }));

                                let dateString = format.format({
                                    value: new Date(),
                                    type: format.Type.DATE,
                                    timezone: format.Timezone.AMERICA_MEXICO_CITY
                                });
                                log.debug('dateString', dateString)
                                addressSubrecord.setText({
                                    fieldId: arrayIds[day],
                                    text: dateString
                                });

                                customerRecord.commitLine({
                                    sublistId: "addressbook"
                                });
                                let id = customerRecord.save();
                                log.debug('se actualizo', id)

                            } catch (error) {
                                log.debug('error al actualizar la fecha', error)
                            }

                            return true;
                        }

                    } else if (weeks.includes(day + 1) || weeks.includes(day - 6)) {
                        //Esta validacion se hizo pensando en el dia siguiente , ya que tiene que verifcar que mañana igual pasa la semana
                        //Si no puede generar el servicio, sin importar que paso la semana, ya que validar que si es 6. restarle -6
                        //Para ver si el siguiente dia es domingo, si no es 6, se le aumenta 1                         
                        let finalDay;
                        if (day == 6) {
                            finalDay = day - 6;
                        } else {
                            finalDay = day + 1;
                        }

                        log.debug('finalDay', finalDay);
                        log.debug('ultima fecha del servicio un dia despues', dates[finalDay]);

                        let converLastDate = format.parse({
                            value: dates[finalDay],
                            type: format.Type.DATE,
                            timezone: format.Timezone.AMERICA_MEXICO_CITY
                        });

                        //Validamos cuantas veces por semana tenemos que hacer el servicio
                        let frecuencyWeek = Number(customer.values['custrecord_ptg_cada.Address']) * 7;
                        log.debug('frecuencyWeek', frecuencyWeek)
                        let lastDate = converLastDate.getDate();
                        log.debug('lastDate', lastDate);
                        //let compareDate = nowDate.setDate(nowDate.getDate() - frecuencyWeek).getDate()
                        //log.debug('plus frecuency week at date now', new Date(nowDate.setDate(nowDate.getDate() - frecuencyWeek)));
                        //Hay que aumentarle una fecha a la de hoy, por que es validar contra el dia siguiente
                        let nextDate = new Date(nowDate.setDate(nowDate.getDate() + 1));
                        log.debug('next Date', nextDate)
                        // //Ahorra si le quitamos los 7 dìas
                        let nextLessDate = new Date(nextDate.setDate(nextDate.getDate() - frecuencyWeek));
                        log.debug('nextLessDate', nextLessDate);
                        log.debug('nextLessDate', nextLessDate.getDate());

                        if (nextLessDate.getDate() == lastDate) {
                            //Tienen la misma fecha y ahora toca definir la hora para saber que turno sera
                            let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                            log.debug('initialhour turn nextDate', initialHour);
                            let hour = getTimeInt(initialHour);
                            log.debug('hour nexDate', hour);
                            (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;

                            let values = {};
                            let fieldId = arrayIds[finalDay]
                            values[fieldId] = nextDate;

                            log.debug('values update nextDate', values);
                            log.debug('customer edited nextDate', customer)

                            // record.submitFields({
                            //     type: 'address',
                            //     id: customer.values['internalid.Address'].value,
                            //     values: values
                            // });
                            try {
                                //Cargar el cliente para actualizar la fecha, pudo ser una funcion pero me dio flojera pasarlo
                                let customerRecord = record.load({
                                    type: record.Type.CUSTOMER,
                                    id: customer.values.internalid.value,
                                    isDynamic: true
                                });

                                let lineAddress = customerRecord.findSublistLineWithValue({
                                    sublistId: 'addressbook',
                                    fieldId: 'addressbookaddress',
                                    value: customer.values["internalid.Address"].value
                                });

                                log.debug('id lineAddress', lineAddress)
                                customerRecord.selectLine({
                                    sublistId: 'addressbook',
                                    line: lineAddress
                                });

                                let addressSubrecord = customerRecord.getCurrentSublistSubrecord({
                                    sublistId: 'addressbook',
                                    fieldId: 'addressbookaddress'
                                });

                                log.debug('test fecha actual', addressSubrecord.getValue({
                                    fieldId: arrayIds[finalDay]
                                }));

                                let dateString = format.format({
                                    value: new Date(new Date().setDate(new Date().getDate() + 1)),
                                    type: format.Type.DATE,
                                    timezone: format.Timezone.AMERICA_MEXICO_CITY
                                });
                                log.debug('dateString', dateString)
                                addressSubrecord.setText({
                                    fieldId: arrayIds[finalDay],
                                    text: dateString
                                });

                                customerRecord.commitLine({
                                    sublistId: "addressbook"
                                });
                                let id = customerRecord.save();
                                log.debug('se actualizo', id)

                            } catch (error) {
                                log.debug('error al actualizar la fecha', error)
                            }

                            return true;
                        }

                    }

                    break;
                //Mensual
                case "3":
                    log.debug('toca mensual', type)
                    //Primero validamos en que semana del mes estamos, con respecto a la fecha de hoy                    
                    let dateActual = date.getDate();
                    let dayActual = date.getDay();

                    let weekOfMonth = Math.ceil((dateActual - 1 - dayActual) / 7) + 1;
                    log.debug('weekOfMonth', weekOfMonth)

                    let weekOfCustomer = customer.values.inThatWeek.value;
                    log.debug('weekOfCustomer', weekOfCustomer)
                    //De igual manera validar en que semana estamos y si estamos en la correcta con respecto al cliente
                    if (weeks.includes(dayActual)) {
                        log.debug('toca hoy', dayActual);

                        //Validamos que ya pasara la cantidad de meses a esperar
                        let monthActual = date.getMonth() + 1;
                        log.debug('monthActual', monthActual)

                        let lastMounthCustomer = format.parse({
                            value: customer.values['custrecord_ptg_fecha_inicio_servicio.Address'],
                            type: format.Type.DATE,
                            timezone: format.Timezone.AMERICA_MEXICO_CITY
                        });

                        log.debug('mes del ultimo servicio', lastMounthCustomer.getMonth() + 1)

                        //Validamos que al sumarle la frecuencia al ultimo mes sea al actual
                        if ((lastMounthCustomer.getMonth() + (1 + Number(customer.values['custrecord_ptg_cada.Address']))) == monthActual) {
                            //Validamos que estemos entre la semana 1 y 4                            
                            if (weekOfMonth <= 5 && weekOfMonth == weekOfCustomer) {

                                //Tienen la misma fecha y ahora toca definir la hora para saber que turno sera
                                let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                                log.debug('initialhour turn nextDate', initialHour);
                                let hour = getTimeInt(initialHour);
                                log.debug('hour nexDate', hour);
                                (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;

                                try {
                                    //Cargar el cliente para actualizar la fecha
                                    let customerRecord = record.load({
                                        type: record.Type.CUSTOMER,
                                        id: customer.values.internalid.value,
                                        isDynamic: true
                                    });

                                    let lineAddress = customerRecord.findSublistLineWithValue({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress',
                                        value: customer.values["internalid.Address"].value
                                    });

                                    log.debug('id lineAddress', lineAddress)
                                    customerRecord.selectLine({
                                        sublistId: 'addressbook',
                                        line: lineAddress
                                    });

                                    let addressSubrecord = customerRecord.getCurrentSublistSubrecord({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress'
                                    });

                                    let dateString = format.format({
                                        value: new Date(),
                                        type: format.Type.DATE,
                                        timezone: format.Timezone.AMERICA_MEXICO_CITY
                                    });
                                    log.debug('dateString', dateString)
                                    addressSubrecord.setText({
                                        fieldId: 'custrecord_ptg_fecha_inicio_servicio',
                                        text: dateString
                                    });

                                    customerRecord.commitLine({
                                        sublistId: "addressbook"
                                    });
                                    let id = customerRecord.save();
                                    log.debug('se actualizo', id)

                                } catch (error) {
                                    log.debug('error al actualizar la fecha', error)
                                }
                                return true;
                            }

                            if (weekOfMonth > 5 && weekOfCustomer == 5) {

                                //Tienen la misma fecha y ahora toca definir la hora para saber que turno sera
                                let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                                log.debug('initialhour turn nextDate', initialHour);
                                let hour = getTimeInt(initialHour);
                                log.debug('hour nexDate', hour);
                                (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;

                                try {
                                    //Cargar el cliente para actualizar la fecha
                                    let customerRecord = record.load({
                                        type: record.Type.CUSTOMER,
                                        id: customer.values.internalid.value,
                                        isDynamic: true
                                    });

                                    let lineAddress = customerRecord.findSublistLineWithValue({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress',
                                        value: customer.values["internalid.Address"].value
                                    });

                                    log.debug('id lineAddress', lineAddress)
                                    customerRecord.selectLine({
                                        sublistId: 'addressbook',
                                        line: lineAddress
                                    });

                                    let addressSubrecord = customerRecord.getCurrentSublistSubrecord({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress'
                                    });

                                    let dateString = format.format({
                                        value: new Date(),
                                        type: format.Type.DATE,
                                        timezone: format.Timezone.AMERICA_MEXICO_CITY
                                    });
                                    log.debug('dateString', dateString)
                                    addressSubrecord.setText({
                                        fieldId: 'custrecord_ptg_fecha_inicio_servicio',
                                        text: dateString
                                    });

                                    customerRecord.commitLine({
                                        sublistId: "addressbook"
                                    });
                                    let id = customerRecord.save();
                                    log.debug('se actualizo', id)

                                } catch (error) {
                                    log.debug('error al actualizar la fecha', error)
                                }
                                return true;

                            }
                        }


                    } else if (weeks.includes(dayActual + 1) || weeks.includes(dayActual - 6)) {
                        let finalDay;
                        if (dayActual == 6) {
                            finalDay = day - 6;
                        } else {
                            finalDay = day + 1;
                        }
                        log.debug('toca mañana', finalDay);
                        //Para el caso del dia siguiente hay que validar igual si el dia de mañana entra en la semana
                        //Validamos que ya pasara la cantidad de meses a esperar
                        let monthActual = date.getMonth() + 1;
                        log.debug('monthActual', monthActual)

                        let lastMounthCustomer = format.parse({
                            value: customer.values['custrecord_ptg_fecha_inicio_servicio.Address'],
                            type: format.Type.DATE,
                            timezone: format.Timezone.AMERICA_MEXICO_CITY
                        });

                        log.debug('mes del ultimo servicio', lastMounthCustomer.getMonth() + 1)

                        //Validamos que al sumarle la frecuencia al ultimo mes sea al actual
                        if ((lastMounthCustomer.getMonth() + (1 + Number(customer.values['custrecord_ptg_cada.Address']))) == monthActual) {
                            //Validamos que estemos entre la semana 1 y 4                            
                            if (weekOfMonth <= 5 && weekOfMonth == weekOfCustomer) {

                                //Tienen la misma fecha y ahora toca definir la hora para saber que turno sera
                                let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                                log.debug('initialhour turn nextDate', initialHour);
                                let hour = getTimeInt(initialHour);
                                log.debug('hour nexDate', hour);
                                (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;

                                try {
                                    //Cargar el cliente para actualizar la fecha
                                    let customerRecord = record.load({
                                        type: record.Type.CUSTOMER,
                                        id: customer.values.internalid.value,
                                        isDynamic: true
                                    });

                                    let lineAddress = customerRecord.findSublistLineWithValue({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress',
                                        value: customer.values["internalid.Address"].value
                                    });

                                    log.debug('id lineAddress', lineAddress)
                                    customerRecord.selectLine({
                                        sublistId: 'addressbook',
                                        line: lineAddress
                                    });

                                    let addressSubrecord = customerRecord.getCurrentSublistSubrecord({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress'
                                    });

                                    let dateString = format.format({
                                        value: new Date(new Date().setDate(new Date().getDate() + 1)),
                                        type: format.Type.DATE,
                                        timezone: format.Timezone.AMERICA_MEXICO_CITY
                                    });
                                    log.debug('dateString', dateString)
                                    addressSubrecord.setText({
                                        fieldId: 'custrecord_ptg_fecha_inicio_servicio',
                                        text: dateString
                                    });

                                    customerRecord.commitLine({
                                        sublistId: "addressbook"
                                    });
                                    let id = customerRecord.save();
                                    log.debug('se actualizo', id)

                                } catch (error) {
                                    log.debug('error al actualizar la fecha', error)
                                }
                                return true;
                            }

                            if (weekOfMonth > 5 && weekOfCustomer == 5) {
                                try {
                                    //Tienen la misma fecha y ahora toca definir la hora para saber que turno sera
                                    let initialHour = customer.values['custrecord_ptg_entre_las.Address'];
                                    log.debug('initialhour turn nextDate', initialHour);
                                    let hour = getTimeInt(initialHour);
                                    log.debug('hour nexDate', hour);
                                    (hour >= 14) ? customer.values.isMoorning = false : customer.values.isMoorning = true;
                                    //Cargar el cliente para actualizar la fecha
                                    let customerRecord = record.load({
                                        type: record.Type.CUSTOMER,
                                        id: customer.values.internalid.value,
                                        isDynamic: true
                                    });

                                    let lineAddress = customerRecord.findSublistLineWithValue({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress',
                                        value: customer.values["internalid.Address"].value
                                    });

                                    log.debug('id lineAddress', lineAddress)
                                    customerRecord.selectLine({
                                        sublistId: 'addressbook',
                                        line: lineAddress
                                    });

                                    let addressSubrecord = customerRecord.getCurrentSublistSubrecord({
                                        sublistId: 'addressbook',
                                        fieldId: 'addressbookaddress'
                                    });

                                    let dateString = format.format({
                                        value: new Date(new Date().setDate(new Date().getDate() + 1)),
                                        type: format.Type.DATE,
                                        timezone: format.Timezone.AMERICA_MEXICO_CITY
                                    });
                                    log.debug('dateString', dateString)
                                    addressSubrecord.setText({
                                        fieldId: 'custrecord_ptg_fecha_inicio_servicio',
                                        text: dateString
                                    });

                                    customerRecord.commitLine({
                                        sublistId: "addressbook"
                                    });
                                    let id = customerRecord.save();
                                    log.debug('se actualizo', id)

                                } catch (error) {
                                    log.debug('error al actualizar la fecha', error)
                                }
                                return true;

                            }
                        }
                    }
                    break;
            }

        }

        //Validar si va a tener ruta matutina o vespertina
        function getTimeInt(time) {
            let timeAux = time.split(" "),
                hour = timeAux[0].split(":")[0];

            if (timeAux[1].toLowerCase() == "pm" || timeAux[1].toLowerCase() == "p.m") {
                if (hour != "12") {
                    hour = parseInt(hour) + 12;
                }
            } else if (timeAux[1].toLowerCase() == "am" || timeAux[1].toLowerCase() == "a.m") {
                if (hour == "12") {
                    hour = 0;
                }
            }
            return parseInt(hour);
        }

        const getWeek = (date) => {
            let initialFormattedDateString = date;
            log.debug('initialFormattedDateString', initialFormattedDateString)
            let parsedDateStringAsRawDateObject = format.parse({
                value: initialFormattedDateString,
                type: format.Type.DATE,
                timezone: format.Timezone.AMERICA_MEXICO_CITY
            });
            //log.debug('date created transform', parsedDateStringAsRawDateObject)
            // log.debug('date created get year', parsedDateStringAsRawDateObject.getFullYear())
            let oneJan = new Date(parsedDateStringAsRawDateObject.getFullYear(), 0, 1);
            let numberOfDays = Math.floor((parsedDateStringAsRawDateObject - oneJan) / (24 * 60 * 60 * 1000));
            let resultWeek = Math.ceil((parsedDateStringAsRawDateObject.getDay() + 1 + numberOfDays) / 7);
            log.debug('result week of year', resultWeek)
            return resultWeek;
        }

        const isMultiple = (initWeek, today, frequency) => {
            let result = false;
            if ((today - initWeek) % frequency == 0) {
                result = true;
            }
            return result;
        }

        //Funciones para crear clindros
        const makeOPCilindro = (typeService, infoCustomer, contactType) => {
            log.debug('makeOPCilindro', 'entro')
            log.debug('makeOPCilindro', infoCustomer)
            log.debug('typeService', typeService)
            log.debug('contactType', contactType)
            let getWeek = new Date().getDay();
            if (getWeek == 0) {
                getWeek = 7;
            }

            if ((contactType == 4 || contactType == 2) && Number(typeService) == 1) {
                //Validamos que tenga configurado un articulo
                //Nota: Se va a cambiar al final el tipo de articulo a nivel de dirección
                if (!!infoCustomer.values['custrecord_ptg_articulo_frecuente.Address']) {
                    //Obtenemos el precio por zona
                    let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    let territory = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getTerritory(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    log.debug('priceZoneValue Cilindro', priceZoneValue)
                    //Obtenemos la cantidad que tiene el articulo de cilindro
                    let itemContent = search.lookupFields({
                        type: 'item',
                        id: Number(infoCustomer.values['custrecord_ptg_articulo_frecuente.Address'].value),
                        columns: ['custitem_ptg_capacidadcilindro_']
                    });
                    log.debug('itemcontent', itemContent)
                    //Creamos la oportunidad
                    let opportunityCilindro = record.create({
                        type: record.Type.OPPORTUNITY,
                        isDynamic: true
                    });

                    opportunityCilindro.setValue('customform', 305);
                    opportunityCilindro.setValue('entity', infoCustomer.values.internalid.value);
                    opportunityCilindro.setValue('custbody_ptg_estado_pedido', (contactType == 4) ? 1 : 6);
                    //opportunityCilindro.setValue('probability', 75);
                    opportunityCilindro.setValue('custbody_ptg_tipo_servicio', 1);
                    opportunityCilindro.setValue('custbody_ptg_id_direccion_envio', infoCustomer.values['addressinternalid.Address']);
                    opportunityCilindro.setValue('custbody_ptg_precio_articulo_zona', priceZoneValue);
                    opportunityCilindro.setText('custbody_ptg_entre_las', infoCustomer.values['custrecord_ptg_entre_las.Address']);
                    opportunityCilindro.setText('custbody_ptg_y_las', infoCustomer.values['custrecord_ptg_y_las.Address']);
                    opportunityCilindro.setValue('custbody_ptg_zonadeprecioop_', territory);
                    opportunityCilindro.setValue('custbody_ptg_planta_relacionada', infoCustomer.values['custentity_ptg_plantarelacionada_'].value);
                    if (infoCustomer.values.isMoorning) {
                        opportunityCilindro.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada.Address'].text);
                    } else {
                        opportunityCilindro.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada2.Address'].text);
                    }
                    //Agregar la direccion de envio
                    opportunityCilindro.setValue('shipaddresslist', Number(infoCustomer.values['addressinternalid.Address']));
                    (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                    let day = [getWeek];
                    //log.debug('day of week', day);
                    // opportunityCilindro.setValue('custbody_ptg_dia_semana', day);
                    //opportunityCilindro.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                    let today = new Date();
                    //today = today.setDate(today.getDate()+1);
                    today.setDate(today.getDate() + 1);
                    //og.debug('today add', today)
                    opportunityCilindro.setValue('expectedclosedate', today)

                    //Validamos que tenga una capacidad del cilindro y que tenga configurado un precio de zona
                    if (!!itemContent['custitem_ptg_capacidadcilindro_'] && Number(priceZoneValue) > 0) {
                        //opportunityCilindro.insertLine({ sublistId: 'item', line: 0 });
                        opportunityCilindro.selectNewLine({ sublistId: 'item' });
                        opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values["custrecord_ptg_articulo_frecuente.Address"].value) });
                        opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values["custrecord_ptg_capacidad_art.Address"]) });
                        let finalRate = Number(itemContent['custitem_ptg_capacidadcilindro_']) * Number(priceZoneValue);
                        opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: finalRate });
                        // let finalAmount = (Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) * Number(itemContent['custitem_ptg_capacidadcilindro_'])) * Number(priceZoneValue);
                        // opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });
                        opportunityCilindro.commitLine({ sublistId: 'item' });
                        let id = opportunityCilindro.save();
                        log.debug('se creo cilindro programada', id)
                    }
                }


            } else if ((contactType == 4 || contactType == 2) && Number(typeService) == 4) {
                //Para el caso de ambos, se tiene que generar una op de clindro que se tiene del primer articulo frecuente
                if (!!infoCustomer.values['custrecord_ptg_articulo_frecuente.Address']) {
                    //Obtenemos el precio por zona
                    let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    let territory = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getTerritory(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    log.debug('priceZoneValue Cilindro', priceZoneValue)
                    //Obtenemos la cantidad que tiene el articulo de cilindro
                    let itemContent = search.lookupFields({
                        type: 'item',
                        id: Number(infoCustomer.values['custrecord_ptg_articulo_frecuente.Address'].value),
                        columns: ['custitem_ptg_capacidadcilindro_']
                    });
                    log.debug('itemcontent', itemContent)
                    //Creamos la oportunidad
                    let opportunityCilindro = record.create({
                        type: record.Type.OPPORTUNITY,
                        isDynamic: true
                    });

                    opportunityCilindro.setValue('customform', 305);
                    opportunityCilindro.setValue('entity', infoCustomer.values.internalid.value);
                    opportunityCilindro.setValue('custbody_ptg_estado_pedido', (contactType == 4) ? 1 : 6);
                    //opportunityCilindro.setValue('probability', 75);
                    opportunityCilindro.setValue('custbody_ptg_tipo_servicio', 1);
                    opportunityCilindro.setValue('custbody_ptg_id_direccion_envio', infoCustomer.values['addressinternalid.Address']);
                    opportunityCilindro.setValue('custbody_ptg_precio_articulo_zona', priceZoneValue);
                    opportunityCilindro.setText('custbody_ptg_entre_las', infoCustomer.values['custrecord_ptg_entre_las.Address']);
                    opportunityCilindro.setText('custbody_ptg_y_las', infoCustomer.values['custrecord_ptg_y_las.Address']);
                    opportunityCilindro.setValue('custbody_ptg_zonadeprecioop_', territory);
                    opportunityCilindro.setValue('custbody_ptg_planta_relacionada', infoCustomer.values['custentity_ptg_plantarelacionada_'].value);
                    if (infoCustomer.values.isMoorning) {
                        opportunityCilindro.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada.Address'].text);
                    } else {
                        opportunityCilindro.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada2.Address'].text);
                    }
                    //Agregar la direccion de envio
                    opportunityCilindro.setValue('shipaddresslist', Number(infoCustomer.values['addressinternalid.Address']));
                    (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                    let day = [getWeek];
                    //log.debug('day of week', day);
                    // opportunityCilindro.setValue('custbody_ptg_dia_semana', day);
                    //opportunityCilindro.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                    let today = new Date();
                    //today = today.setDate(today.getDate()+1);
                    today.setDate(today.getDate() + 1);
                    //og.debug('today add', today)
                    opportunityCilindro.setValue('expectedclosedate', today)

                    //Validamos que tenga una capacidad del cilindro y que tenga configurado un precio de zona
                    if (!!itemContent['custitem_ptg_capacidadcilindro_'] && Number(priceZoneValue) > 0) {
                        //opportunityCilindro.insertLine({ sublistId: 'item', line: 0 });
                        opportunityCilindro.selectNewLine({ sublistId: 'item' });
                        opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values["custrecord_ptg_articulo_frecuente.Address"].value) });
                        opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values["custrecord_ptg_capacidad_art.Address"]) });
                        let finalRate = Number(itemContent['custitem_ptg_capacidadcilindro_']) * Number(priceZoneValue);
                        opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: finalRate });
                        // let finalAmount = (Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil) * Number(itemContent['custitem_ptg_capacidadcilindro_'])) * Number(priceZoneValue);
                        // opportunityCilindro.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });
                        opportunityCilindro.commitLine({ sublistId: 'item' });
                        let id = opportunityCilindro.save();
                        log.debug('se creo cilindro programada ambos', id)
                    }
                }

            }
        }

        //Funcion especial para los de tipo Programado, hay que validar que su limite de credito no sea mayor al agg amount
        const validCredit = () => {

        }

        //Funcion para crear estacionario
        const makeOPEstacionario = (typeService, infoCustomer, contactType) => {
            log.debug('makeOPEstacionario', 'entro')
            let getWeek = new Date().getDay();
            if (getWeek == 0) {
                getWeek = 7;
            }

            if ((contactType == 4 || contactType == 2) && Number(typeService) == 2) {
                //Mismo proceso que cilindro pero ahora con estacionario y la diferencia de momento es que no tiene configurado una capacidad por el tipo de articulo
                if (!!infoCustomer.values['custrecord_ptg_articulo_frecuente.Address']) {
                    let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    let territory = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getTerritory(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    let opportunityEstacionaria = record.create({
                        type: record.Type.OPPORTUNITY,
                        isDynamic: true
                    });

                    opportunityEstacionaria.setValue('customform', 305);
                    opportunityEstacionaria.setValue('entity', infoCustomer.values.internalid.value);
                    opportunityEstacionaria.setValue('custbody_ptg_estado_pedido', (contactType == 4) ? 1 : 6);
                    //opportunityEstacionaria.setValue('probability', 75);
                    opportunityEstacionaria.setValue('custbody_ptg_tipo_servicio', 2);
                    opportunityEstacionaria.setValue('custbody_ptg_precio_articulo_zona', priceZoneValue);
                    log.debug('address id estacionario', infoCustomer.values['addressinternalid.Address'])
                    opportunityEstacionaria.setValue('custbody_ptg_id_direccion_envio', infoCustomer.values['addressinternalid.Address']);
                    opportunityEstacionaria.setText('custbody_ptg_entre_las', infoCustomer.values['custrecord_ptg_entre_las.Address']);
                    opportunityEstacionaria.setText('custbody_ptg_y_las', infoCustomer.values['custrecord_ptg_y_las.Address']);
                    opportunityEstacionaria.setValue('custbody_ptg_zonadeprecioop_', territory);
                    opportunityEstacionaria.setValue('custbody_ptg_planta_relacionada', infoCustomer.values['custentity_ptg_plantarelacionada_'].value);
                    if (infoCustomer.values.isMoorning) {
                        opportunityEstacionaria.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada.Address'].text);
                    } else {
                        opportunityEstacionaria.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada2.Address'].text);
                    }
                    //Agregar la direccion de envio
                    opportunityEstacionaria.setValue('shipaddresslist', Number(infoCustomer.values['addressinternalid.Address']));
                    (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                    let day = [getWeek];
                    //log.debug('day of week', day);
                    //opportunityEstacionaria.setValue('custbody_ptg_dia_semana', day);
                    //opportunityEstacionaria.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                    let today = new Date();
                    //today = today.setDate(today.getDate()+1);
                    today.setDate(today.getDate() + 1);
                    //log.debug('today add', today)
                    opportunityEstacionaria.setValue('expectedclosedate', today)

                    if (Number(priceZoneValue) > 0) {
                        //opportunityEstacionaria.insertLine({ sublistId: 'item', line: 0 });
                        opportunityEstacionaria.selectNewLine({ sublistId: 'item' });
                        opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values["custrecord_ptg_articulo_frecuente.Address"].value) });
                        opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values["custrecord_ptg_capacidad_art.Address"]) });
                        opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: Number(priceZoneValue) });
                        // let finalAmount = Number(priceZoneValue) * Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil);
                        // opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });                                        
                        opportunityEstacionaria.commitLine({ sublistId: 'item' });
                        let id = opportunityEstacionaria.save();
                        log.debug('se creo programado estacionaria', id)
                    }
                }

            }
            else if ((contactType == 4 || contactType == 2) && Number(typeService) == 4) {
                //Mismo proceso que cilindro pero ahora con estacionario y la diferencia de momento es que no tiene configurado una capacidad por el tipo de articulo
                //Y en este caso se utiliza el campo de segundo articulo frecuente
                if (!!infoCustomer.values['custrecord_ptg_articulo_frecuente2.Address']) {
                    let priceZoneValue = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getPriceZone(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    let territory = (!!infoCustomer.values['custrecord_ptg_colonia_ruta.Address']) ? getTerritory(infoCustomer.values['custrecord_ptg_colonia_ruta.Address'].value) : 0;
                    let opportunityEstacionaria = record.create({
                        type: record.Type.OPPORTUNITY,
                        isDynamic: true
                    });

                    opportunityEstacionaria.setValue('customform', 305);
                    opportunityEstacionaria.setValue('entity', infoCustomer.values.internalid.value);
                    opportunityEstacionaria.setValue('custbody_ptg_estado_pedido', (contactType == 4) ? 1 : 6);
                    //opportunityEstacionaria.setValue('probability', 75);
                    opportunityEstacionaria.setValue('custbody_ptg_tipo_servicio', 2);
                    opportunityEstacionaria.setValue('custbody_ptg_precio_articulo_zona', priceZoneValue);
                    log.debug('address id estacionario', infoCustomer.values['addressinternalid.Address'])
                    opportunityEstacionaria.setValue('custbody_ptg_id_direccion_envio', infoCustomer.values['addressinternalid.Address']);
                    opportunityEstacionaria.setText('custbody_ptg_entre_las', infoCustomer.values['custrecord_ptg_entre_las.Address']);
                    opportunityEstacionaria.setText('custbody_ptg_y_las', infoCustomer.values['custrecord_ptg_y_las.Address']);
                    opportunityEstacionaria.setText('custbody_ptg_y_las', infoCustomer.values['custrecord_ptg_y_las.Address']);
                    opportunityEstacionaria.setValue('custbody_ptg_zonadeprecioop_', territory);
                    opportunityEstacionaria.setValue('custbody_ptg_planta_relacionada', infoCustomer.values['custentity_ptg_plantarelacionada_'].value);
                    if (infoCustomer.values.isMoorning) {
                        opportunityEstacionaria.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada_3.Address'].text);
                    } else {
                        opportunityEstacionaria.setValue('custbody_ptg_ruta_asignada', infoCustomer.values['custrecord_ptg_ruta_asignada_4.Address'].text);
                    }
                    //Agregar la direccion de envio
                    opportunityEstacionaria.setValue('shipaddresslist', Number(infoCustomer.values['addressinternalid.Address']));
                    (getWeek == 0) ? getWeek = 7 : getWeek = getWeek;
                    let day = [getWeek];
                    //log.debug('day of week', day);
                    //opportunityEstacionaria.setValue('custbody_ptg_dia_semana', day);
                    //opportunityEstacionaria.setValue('subsidiary', infoCustomer.values.subsidiary.value);
                    let today = new Date();
                    //today = today.setDate(today.getDate()+1);
                    today.setDate(today.getDate() + 1);
                    //log.debug('today add', today)
                    opportunityEstacionaria.setValue('expectedclosedate', today)

                    if (Number(priceZoneValue) > 0) {
                        //opportunityEstacionaria.insertLine({ sublistId: 'item', line: 0 });
                        opportunityEstacionaria.selectNewLine({ sublistId: 'item' });
                        opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'item', value: Number(infoCustomer.values["custrecord_ptg_articulo_frecuente2.Address"].value) });
                        opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'quantity', value: Number(infoCustomer.values["custrecord_ptg_capacidad_can_articulo_2.Address"]) });
                        opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'rate', value: Number(priceZoneValue) });
                        // let finalAmount = Number(priceZoneValue) * Number(infoCustomer.values.custentity_ptg_cantidad_frecuente_lt_cil);
                        // opportunityEstacionaria.setCurrentSublistValue({ sublistId: 'item', fieldId: 'amount', value: finalAmount });                                        
                        opportunityEstacionaria.commitLine({ sublistId: 'item' });
                        let id = opportunityEstacionaria.save();
                        log.debug('se creo programado estacionaria ambos', id)
                    }
                }
            }



        }



        const validExistOp = (customer, type, address) => {
            log.debug('validExistOp', address)
            log.debug('test de mac')
            log.debug('type validExistOp', type)

            let isExiste = false;
            let filters;
            if (type == 2) {
                filters = [
                    ["entity", "anyof", customer],
                    "AND",
                    ["custbody_ptg_id_direccion_envio", "is", address],
                    "AND",
                    ["custbody_ptg_estado_pedido", "noneof", "5", "3"]
                    // "AND",
                    // ["date", "within", "today"],
                    // "OR",
                    // ["date", "within", "tomorrow"],
                    // "AND",
                    // ["custbody_ptg_estado_pedido", "noneof", "5", "3"],

                ]
            } else if (type == 4) {
                filters = [
                    ["entity", "anyof", customer],
                    "AND",
                    ["custbody_ptg_estado_pedido", "noneof", "5", "3"]
                    // "AND",
                    // // ["date", "within", "today"],
                    // // "AND",
                    // ["custbody_ptg_estado_pedido", "noneof", "5", "3"],
                    // "AND",
                    // ["custbody_ptg_id_direccion_envio", "is", address]
                ]
            }

            log.debug('filtros validaciónExistop', filters)

            let opportunitySearchObj = search.create({
                type: "opportunity",
                filters: filters,
                columns:
                    [
                        search.createColumn({ name: "internalid", label: "Internal ID" }),
                        search.createColumn({ name: "transactionnumber", label: "Transaction Number" })
                    ]
            });
            let searchResultCount = opportunitySearchObj.runPaged().count;

            if (searchResultCount > 0) {
                log.debug('searchresult exist', searchResultCount);
                isExiste = true;

            }
            return isExiste
        }

        const getGenericItem = (type, subsidiary) => {
            let filters;
            switch (type) {
                case '1':
                    filters = [["type", "anyof", "InvtPart"],
                        "AND",
                    ["custitem_ptg_tipodearticulo_", "anyof", "1"],
                        "AND",
                    ["subsidiary", "anyof", subsidiary]]
                    break;
                case '2':
                    filters = [["type", "anyof", "InvtPart"],
                        "AND",
                    ["custitem_ptg_tipodearticulo_", "anyof", "2"],
                        "AND",
                    ["subsidiary", "anyof", subsidiary]]
                    break;

                //Este queda pendiente del global de momento se dejara 4 , hasta que se cambie el nombre
                case '4':
                    filters = [["type", "anyof", "InvtPart"],
                        "AND",
                    ["custitem_ptg_tipodearticulo_", "anyof", "1"],
                        "AND",
                    ["subsidiary", "anyof", subsidiary]]
                    break;

            }

            //log.debug('filtros articulos genericos', filters)

            let inventoryitemSearchObj = search.create({
                type: "inventoryitem",
                filters: filters,
                columns:
                    [
                        search.createColumn({
                            name: "itemid",
                            sort: search.Sort.ASC,
                            label: "Name"
                        }),
                        search.createColumn({ name: "displayname", label: "Display Name" }),
                        search.createColumn({ name: "salesdescription", label: "Description" }),
                        search.createColumn({ name: "type", label: "Type" }),
                        search.createColumn({ name: "baseprice", label: "Base Price" }),
                        search.createColumn({ name: "custitem_4601_defaultwitaxcode", label: "Default WT Code" })
                    ]
            });
            let searchResultCount = inventoryitemSearchObj.runPaged().count;
            if (searchResultCount > 0) {
                let item;
                inventoryitemSearchObj.run().each(function (result) {
                    // .run().each has a limit of 4,000 results
                    item = result.id
                    log.debug('result', result)

                    return false;
                });

                return item;
            }

        }

        const getPriceZone = (zone) => {
            log.debug('zone', zone);
            let customrecord_ptg_coloniasrutas_SearchObj = search.create({
                type: "customrecord_ptg_coloniasrutas_",
                filters:
                    [
                        ["internalid", "anyof", zone]
                    ],
                columns:
                    [
                        search.createColumn({
                            name: "custrecord_ptg_precio_",
                            join: "CUSTRECORD_PTG_ZONA_DE_PRECIO_",
                            label: "PTG - PRECIO"
                        }),
                        search.createColumn({
                            name: "custrecord_ptg_territorio_",
                            join: "CUSTRECORD_PTG_ZONA_DE_PRECIO_",
                            label: "PTG - Territorio"
                        }),
                        search.createColumn({ name: "custrecord_ptg_zona_de_precio_", label: "PTG - Zona de Precio" }),
                    ]
            });
            let searchResultCount = customrecord_ptg_coloniasrutas_SearchObj.runPaged().count;
            if (searchResultCount > 0) {
                let price;
                customrecord_ptg_coloniasrutas_SearchObj.run().each(function (result) {
                    // .run().each has a limit of 4,000 results
                    log.debug('result zone', result)
                    price = result.getValue({
                        name: "custrecord_ptg_precio_",
                        join: "CUSTRECORD_PTG_ZONA_DE_PRECIO_",
                        label: "PTG - PRECIO"
                    })
                    return true;
                });

                return price;
            }


        }

        const getTerritory = (zone) => {
            //log.debug('zone', zone);
            let customrecord_ptg_coloniasrutas_SearchObj = search.create({
                type: "customrecord_ptg_coloniasrutas_",
                filters:
                    [
                        ["internalid", "anyof", zone]
                    ],
                columns:
                    [
                        search.createColumn({
                            name: "custrecord_ptg_precio_",
                            join: "CUSTRECORD_PTG_ZONA_DE_PRECIO_",
                            label: "PTG - PRECIO"
                        }),
                        search.createColumn({
                            name: "custrecord_ptg_territorio_",
                            join: "CUSTRECORD_PTG_ZONA_DE_PRECIO_",
                            label: "PTG - Territorio"
                        }),
                        search.createColumn({ name: "custrecord_ptg_zona_de_precio_", label: "PTG - Zona de Precio" }),
                    ]
            });
            let searchResultCount = customrecord_ptg_coloniasrutas_SearchObj.runPaged().count;
            if (searchResultCount > 0) {
                let territory;
                customrecord_ptg_coloniasrutas_SearchObj.run().each(function (result) {
                    // .run().each has a limit of 4,000 results
                    //log.debug('result zone', result)
                    territory = result.getValue({
                        name: "custrecord_ptg_zona_de_precio_", label: "PTG - Zona de Precio"
                    })
                    return true;
                });

                return territory;
            }


        }


        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {

        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {
            const thereAreAnyError = (summaryContext) => {
                const inputSummary = summaryContext.inputSummary;
                const mapSummary = summaryContext.mapSummary;
                const reduceSummary = summaryContext.reduceSummary;
                //si no hay errores entonces se sale del la función y se retorna false incando que no hubo errores            if (!inputSummary.error) return false;
                //se hay errores entonces se imprimen los errores en el log para poder visualizarlos            if (inputSummary.error) log.debug("ERROR_INPPUT_STAGE", `Erro: ${inputSummary.error}`);
                handleErrorInStage('map', mapSummary);
                handleErrorInStage('reduce', reduceSummary);

                function handleErrorInStage(currentStage, summary) {
                    summary.errors.iterator().each((key, value) => {
                        log.debug(`ERROR_${currentStage}`, `Error( ${currentStage} ) with key: ${key}.Detail: ${JSON.parse(value).message}`);
                        return true;
                    });
                }
                return true;
            };

            thereAreAnyError(summaryContext)
            log.audit('Summary', [
                { title: 'Usage units consumed', details: summaryContext.usage },
                { title: 'Concurrency', details: summaryContext.concurrency },
                { title: 'Number of yields', details: summaryContext.yields }
            ]);

        }

        return { getInputData, map, reduce, summarize }

    });
