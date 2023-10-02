package com.suitesoftware.psa.catalogservice;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.suitesoftware.psa.catalogservice.dto.CatalogCustomer;
import com.suitesoftware.psa.catalogservice.impl.CatalogManagerImpl;
import junit.framework.TestCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class CatalogManagerTest {

    @Mock
    CatalogDao catalogDao;

    static CatalogCustomer catalogCustomer5 = new CatalogCustomer();
    {catalogCustomer5.setCustomerId(5);}

    @Test
    public void testGetCatalogCustomer() {

        when(catalogDao.getCustomer(5)).thenReturn(catalogCustomer5);

        CatalogManagerImpl catalogManager = new CatalogManagerImpl();
        catalogManager.setCatalogDao(catalogDao);
        CatalogCustomer cc = catalogManager.getCustomer(5);
        assertEquals(cc.getCustomerId(),5);
    }

}
